import os
import warnings
import tempfile
import re
from functools import lru_cache, partial
from sqlalchemy import text
from sqlalchemy.sql.expression import literal
from multiprocessing import Manager, Process
from time import time, sleep
from loguru import logger

import numpy as np
import pandas as pd
from click import echo
from pgcopy import CopyManager
from psycopg2.errors import UniqueViolation, OperationalError
from tqdm import tqdm

from lightcurvedb import db_from_config
from lightcurvedb.models.table_track import RangedPartitionTrack
from lightcurvedb.core.engines import psycopg_connection
from lightcurvedb.core.ingestors.cache import IngestionCache
from lightcurvedb.core.ingestors.temp_table import FileObservation, TIC8Parameters
from lightcurvedb.core.ingestors.lightcurve_ingestors import (
    get_h5,
    get_h5_data,
    get_correct_qflags,
    get_lightcurve_median,
    get_tjd,
    get_components,
    h5_to_numpy
)
from lightcurvedb.core.psql_tables import PGClass, PGNamespace
from lightcurvedb.legacy.timecorrect import TimeCorrector
from lightcurvedb.models import (
    Frame,
    Lightpoint,
    Observation,
    Orbit,
)
from lightcurvedb.util.decorators import track_runtime

LC_ERROR_TYPES = {"RawMagnitude"}


def yield_lp_kwarg_from_merge_jobs(
    normalizer, stellar_params, merge_job, config_override=None
):
    tmag = stellar_params.loc[merge_job.tic_id]["tmag"]
    # Load in h5 data
    lp_raw_kwargs, file_time = get_h5_data(merge_job)

    # Load in manual quality flags
    qflags, qflag_time = get_correct_qflags(
        merge_job, lp_raw_kwargs["cadence"]
    )

    # Align orbit data to tmag median
    aligned_data, align_time = get_lightcurve_median(
        lp_raw_kwargs["data"], qflags, tmag
    )

    # Pull tjd data from Frame table and correct for earth time
    tjd, tjd_pull_time = get_tjd(
        merge_job, lp_raw_kwargs["cadence"], config_override=config_override
    )
    correct_bjd = normalizer.correct(merge_job.tic_id, tjd)

    lp_raw_kwargs["data"] = aligned_data
    lp_raw_kwargs["barycentric_julian_date"] = correct_bjd
    lp_raw_kwargs["quality_flag"] = qflags
    lp_raw_kwargs["lightcurve_id"] = np.full_like(
        lp_raw_kwargs["cadence"], merge_job.lightcurve_id, dtype=int
    )

    timings = {
        "file_load": file_time,
        "quality_flag_assignment": qflag_time,
        "bjd_correction": tjd_pull_time,
        "alignment": align_time,
    }

    return lp_raw_kwargs, timings


def yield_lp_df_from_merge_jobs(
    normalizer, stellar_params, single_merge_jobs, config_override=None
):
    for job in single_merge_jobs:
        lp_raw_kwargs, timings = yield_lp_kwarg_from_merge_jobs(
            normalizer, stellar_params, job, config_override=config_override
        )

        lp = pd.DataFrame(data=lp_raw_kwargs)

        yield lp, timings


def push_lightpoints(mgr, lp_arr):
    timing = {}
    stamp = time()
    mgr.threading_copy(lp_arr)
    timing["copy_elapsed"] = time() - stamp
    timing["n_lightpoints"] = len(lp_arr)
    return timing


def push_observations(conn, observations):
    timing = {}
    with conn.cursor() as cur:
        values_iter = map(
            lambda row: f"({row[0]}, {row[1]}, {row[2]}, {row[3]})",
            observations
        )
        value_str = ", ".join(values_iter)
        stamp = time()
        cur.execute(
            "INSERT INTO observations ("
            "lightcurve_id, orbit_id, camera, ccd"
            f") VALUES {value_str}"
        )
        timing["obs_insertion_time"] = time() - stamp
    return timing


class LightpointIngestor:
    normalizer = None
    quality_flag_map = None
    mid_tjd_map = None
    job_queue = None
    name = "Worker"

    def __init__(self, db, name):
        self.name = name
        self.db = db

    def log(self, msg, level="debug"):
        full_msg = f"{self.name} {msg}"
        getattr(logger, level, logger.debug)(fill_msg)

    def _load_contexts(self):
        with self.db as db:
            cache = IngestionCache()
            self.normalizer = TimeCorrector(db, cache)
            self.log("Instantiated bjd normalizer")
            self.quality_flag_map = cache.quality_flag_map
            self.log("Instantiated quality-flag cadence map")
            self.mid_tjd_map = db.get_mid_tjd_mapping()
            self.log("Instantiated mid-tjd cadence map")
            self.orbit_map = dict(db.query(Orbit.orbit_number, Orbit.id))
            self.log("Instantiated Orbit ID map")
            cache.session.close()

    def get_quality_flags(self, camera, ccd, cadences):
        qflag_df = self.quality_flag_map[(camera, ccd)].loc[cadences]
        return qflag_df.quality_flag.to_numpy()

    def get_mid_tjd(self, camera, cadences):
        return self.mid_tjd_map[camera].loc[cadences]["mid_tjd"].to_numpy()

    def get_mag_alignment_offset(self, data, quality_flags, tmag):
        mask = quality_flags == 0
        return np.nanmedian(data[mask]) - tmag

    def process_h5(self, id_, aperture, lightcurve_type, h5path):
        self.log(f"Processing {h5path}", level="trace")
        lightcurve = h5_to_numpy(id_, aperture, lightcurve_type, h5path)
        context = get_components(h5path)
        tic_id, camera, ccd = (
            int(context["tic_id"]),
            int(context["camera"]),
            int(context["ccd"])
        )
        tmag = self.corrector.tic_parameters.loc[tic_id]["tmag"]
        cadences = lightcurve["cadence"]
        data = lightcurve["data"]

        quality_flags = self.get_quality_flags(camera, ccd, cadence)
        mid_tjd = self.get_mid_tjd(camera, cadences)
        bjd = self.normalizer.correct(tic_id, mid_tjd)
        mag_offset = self.get_mag_alignment_offset(data, quality_flags, tmag)
        aligned_mag = data - offset

        lightcurve["quality_flag"] = quality_flags
        lightcurve["barycentric_julian_date"] = bjd
        lightcurve["data"] = aligned_mag
        return lightcurve

    def process_single_merge_job(self, smj):
        orbit_number = smj.orbit_number
        lightcurve_id = smj.lightcurve_id

        if (lightcurve_id, orbit_number) in self.seen_cache:
            self.log(
                f"Already seen {lightcurve_id} for orbit {orbit_number}, ignoring {smj}",
                level="warning"
            )
            return None
        try:
            lightpoint_array = self.process_h5(
                smj.lightcurve_id,
                smj.aperture,
                smj.lightcurve_type,
                smj.file_path
            )
            observation = (
                smj.lightcurve_id,
                smj.orbit_id,
                smj.camera,
                smj.ccd
            )
            self.buffers["lightpoint"].append(lightpoint_array)
            self.buffers["observation"].append(observation)
        except OSError:
            self.log(
                f"Unable to open {smj.file_path}",
                level="error"
            )
        finally:
            self.seen_cache.add((lightcurve_id, orbit_number))

    def process_job(self, job):
        for single_merge_job in job.single_merge_jobs:
            self.process_single_merge_job(single_merge_job)
            if self.should_flush:
                self.flush()

    def flush(self):
        with self.db as db:
            conn = db.session.connection().connection
            lp_flush_stats = self.flush_lightpoints(conn)
            obs_flush_stats = self.flush_observations(conn)
            conn.commit()

    def flush_lightpoints(self, conn):
        raise NotImplementedError

    def flush_observations(self, conn):
        raise NotImplementedError

    def stage_merge(self, single_merge_job):
        raise NotImplementedError

    def run(self):
        self.log("Entering main runtime")
        self._load_contexts()
        pass

    @property
    def should_flush(self):
        pass


class LightpointProcessor(Process):

    lp_buffer = []
    obs_buffer = []
    ingestion_parameters = {}

    def __init__(self, *args, **kwargs):
        super(LightpointProcessor, self).__init__(**kwargs)
        self.logger_name = kwargs.pop("logger", "lightcurvedb")
        self.prefix = "LP Processor"


    def log(self, msg, level="debug", exc_info=False):
        getattr(logger, level.lower())(
            "{0}: {1}".format(self.name, msg), exc_info=exc_info
        )

    def set_name(self):
        self.name = "{0}-{1}".format(self.prefix, os.getpid())


class PartitionConsumer(LightpointProcessor):
    prefix = "Copier"

    def __init__(
        self,
        db,
        qflag_map,
        mid_tjd,
        normalizer,
        job_queue,
        result_queue,
        **kwargs
    ):

        temp_dir = kwargs.pop("temp_dir", os.path.join("/", "scratch", "tmp", "lcdb_ingestion"))

        super(PartitionConsumer, self).__init__(**kwargs)

        self.orbit_map = dict(db.query(Orbit.orbit_number, Orbit.id))
        self.job_queue = job_queue
        self.result_queue = result_queue
        self.corrector = normalizer
        self.qflag_map = qflag_map
        self.mid_tjd_map = mid_tjd
        self.temp_dir = temp_dir

        self.cached_get_qflags = None
        self.cached_get_correct_mid_tjd = None

    def get_qflags(self, h5path):
        cadences = get_h5(h5path)["LightCurve"]["Cadence"][()].astype(int)
        context = get_components(h5path)
        camera, ccd = context["camera"], context["ccd"]
        qflag_df = self.qflag_map[(camera, ccd)].loc[cadences]

        return qflag_df.quality_flag.to_numpy()

    def get_correct_mid_tjd(self, h5path):
        cadences = get_h5(h5path)["LightCurve"]["Cadence"][()].astype(int)
        context = get_components(h5path)
        tic_id, camera = (
            context["tic_id"],
            context["camera"],
        )
        mid_tjd = self.mid_tjd_map[camera].loc[cadences]["mid_tjd"].to_numpy()

        return self.corrector.correct(tic_id, mid_tjd)

    def process_h5(self, id_, aperture, lightcurve_type, h5path):
        self.log("Processing {0}".format(h5path), level="trace")
        timings = {}
        stamp = time()
        context = get_components(h5path)
        tic_id, camera, ccd = (
            int(context["tic_id"]),
            int(context["camera"]),
            int(context["ccd"])
        )
        tmag = self.corrector.tic_parameters.loc[context["tic_id"]]["tmag"]
        lightcurve = h5_to_numpy(id_, aperture, lightcurve_type, h5path)
        timings["file_load"] = time() - stamp

        stamp = time()
        lightcurve["quality_flag"] = self.cached_get_qflags(h5path)
        timings["quality_flag_assignment"] = time() - stamp
        stamp = time()
        lightcurve["barycentric_julian_date"] = self.cached_get_correct_mid_tjd(h5path)
        timings["bjd_correction"] = time() - stamp

        stamp = time()
        mask = lightcurve["quality_flag"] == 0
        good_values = lightcurve[mask]["data"]
        offset = np.nanmedian(good_values) - tmag
        lightcurve["data"] = lightcurve["data"] - offset
        timings["alignment"] = time() - stamp

        return lightcurve, timings

    def ingest_data(self, partition_relname, lightpoints, observations):
        ingested = False
        backoff = 1
        conn = None
        while not ingested:
            try:
                conn = psycopg_connection()
                self.log("Pushing data to {0}".format(partition_relname))
                mgr = CopyManager(conn, partition_relname, Lightpoint.get_columns())

                stamp = time()

                # define temp file
                mgr.threading_copy(lightpoints)

                mgr = CopyManager(
                    conn,
                    Observation.__tablename__,
                    ["lightcurve_id", "orbit_id", "camera", "ccd"],
                )
                self.log("Pushing new observations")
                mgr.threading_copy(observations)

                copy_elapsed = time() - stamp
                conn.commit()
                ingested = True
            except UniqueViolation as e:
                conn.rollback()
                self.log("Needing to reduce ingestion due to {0}".format(e), level="error")
                match = re.search("\((?P<orbit_id>\d+),\s*(?P<lightcurve_id>\d+)\)", str(e))
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT MIN(cadence), MAX(cadence) FROM frames WHERE orbit_id = {0} AND frame_type_id = 'Raw FFI'".format(
                            int(match["orbit_id"])
                        )
                    )
                    min_cadence, max_cadence = cursor.fetchone()
                observations = list(
                    filter(
                        lambda obs: not (obs[0] == int(match["lightcurve_id"]) and obs[1] == int(match["orbit_id"])),
                        observations 
                    )
                )
                id_mask = (lightpoints["lightcurve_id"] == int(match["lightcurve_id"]))
                cadence_mask = (min_cadence <= lightpoints["cadence"]) & (lightpoints["cadence"] <= max_cadence)
                lightpoints = lightpoints[~(id_mask & cadence_mask)]
                self.log("Reduced ingestion work", level="warning")
            except OperationalError as e:
                self.log(
                    "Encountered {0}, performing backoff with wait of {1}s".format(e, backoff),
                    level="error"
                )
                sleep(backoff)
                backoff *= 2
            finally:
                if conn:
                    conn.close()
        return copy_elapsed

    def process_job(self, partition_job):
        lps = []
        observations = []
        timings = []
        validation_time = 0
        total_points = 0
        copy_elapsed = 0

        seen_cache = set()

        optimized_path = sorted(
            partition_job.single_merge_jobs,
            key=lambda job: (job.lightcurve_id, job.orbit_number),
        )
        with db_from_config() as db:
            q = (
                db
                .query(PGNamespace.nspname, PGClass.relname)
                .join(PGClass.namespace)
                .filter(
                    PGClass.oid == partition_job.partition_oid
                )
            )
            namespace, tablename = q.one()
            target_table = "{0}.{1}".format(namespace, tablename)

        for lc_job in optimized_path:
            if (lc_job.lightcurve_id, lc_job.orbit_number) in seen_cache:
                continue
            try:
                lp, timing = self.process_h5(
                    lc_job.lightcurve_id,
                    lc_job.aperture,
                    lc_job.lightcurve_type,
                    lc_job.file_path,
                )
            except OSError:
                self.log(
                    "Unable to open {0}".format(lc_job.file_path),
                    level="error",
                )
                timing = {
                    "file_load": 0,
                    "quality_flag_assignment": 0,
                    "bjd_correction": 0,
                    "alignment": 0,
                }
                continue
            finally:
                timings.append(timing)

            stamp = time()
            # remove duplicates
            path = np.argsort(lp, order=["lightcurve_id", "cadence"])
            val_diff = np.insert(
                np.diff(lp["cadence"][path]),
                0,
                1
            )
            # Traverse path where there are no duplicates
            lp = lp[path[val_diff != 0]]
            validation_time += time() - stamp

            if len(lp) > 0:
                orbit_id = self.orbit_map[lc_job.orbit_number]
                observations.append(
                    (
                        lc_job.lightcurve_id,
                        orbit_id,
                        lc_job.camera,
                        lc_job.ccd,
                    )
                )
                lps.append(lp)
                seen_cache.add((lc_job.lightcurve_id, lc_job.orbit_number))

        if not lps:
            result = dict(pd.DataFrame(timings).sum())
            result["relname"] = target_table
            result["n_lightpoints"] = 0
            result["validation"] = validation_time
            result["copy_elapsed"] = copy_elapsed
            result["n_jobs"] = len(partition_job.single_merge_jobs)
            return result

        copy_elapsed += self.ingest_data(
            target_table, np.concatenate(lps), observations
        )

        total_points = sum(len(lp) for lp in lps)
        result = dict(pd.DataFrame(timings).sum())
        result["relname"] = target_table
        result["n_lightpoints"] = total_points
        result["validation"] = validation_time
        result["copy_elapsed"] = copy_elapsed
        result["n_jobs"] = len(partition_job.single_merge_jobs)

        return result

    def run(self):
        self.set_name()
        self.log("Initialized")
        self.cached_get_qflags = lru_cache(maxsize=16)(self.get_qflags)
        self.cached_get_correct_mid_tjd = lru_cache(maxsize=16)(
            self.get_correct_mid_tjd
        )
        self.log("Initialized lru h5 readers")

        job = self.job_queue.get()
        self.log("First job obtained")
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore", r"All-NaN (slice|axis) encountered"
            )
            while job is not None:
                results = self.process_job(job)
                self.result_queue.put(results)
                self.job_queue.task_done()
                job = self.job_queue.get()

        self.job_queue.task_done()
        self.log("Received end-of-work signal")


class LightcurveConsumer(PartitionConsumer):
    def process_job(self, lightcurve_job):
        lps = []
        observations = []
        timings = []
        validation_time = 0
        total_points = 0
        copy_elapsed = 0

        seen_cache = set()
        optimized_path = sorted(
            lightcurve_job.single_merge_jobs,
            key=lambda job: (job.orbit_number)
        )

        with db_from_config() as db:
            q = (
                db
                .query(PGNamespace.nspname, PGClass.relname)
                .join(PGClass.namespace)
                .join(RangedPartitionTrack, PGClass.oid == RangedPartitionTrack.oid)
                .filter(
                    literal(
                        lightcurve_job.lightcurve_id
                    )
                    .between(
                        RangedPartitionTrack.min_range,
                        RangedPartitionTrack.max_range - 1,
                    )
                )
            )
            namespace, tablename = q.one()
            target_table = "{0}.{1}".format(namespace, tablename)

        for single_merge_job in optimized_path:
            if (single_merge_job.lightcurve_id, single_merge_job.orbit_number) in seen_cache:
                continue
            try:
                self.log(
                    "Processing {0}".format(single_merge_job.file_path),
                    level="trace"
                )
                lp, timing = self.process_h5(
                    single_merge_job.lightcurve_id,
                    single_merge_job.aperture,
                    single_merge_job.lightcurve_type,
                    single_merge_job.file_path,
                )
            except OSError:
                self.log(
                    "Unable to read {0} for ingest".format(single_merge_job.file_path)
                )
                continue
            timings.append(timing)
            stamp = time()
            lp.set_index(["lightcurve_id", "cadence"], inplace=True)
            lp = lp[~lp.index.duplicated(keep="last")]
            validation_time += time() - stamp
            if not lp.empty:
                orbit_id = self.orbit_map[single_merge_job.orbit_number]
                observations.append(
                    (
                        single_merge_job.lightcurve_id,
                        orbit_id,
                        single_merge_job.camera,
                        single_merge_job.ccd
                    )
                )
                lps.append(lp)
                seen_cache.add((
                    single_merge_job.lightcurve_id,
                    single_merge_job.orbit_number
                ))
        if not lps:
            result = dict(pd.DataFrame(timings).sum())
            result["relname"] = tablename
            result["n_lightpoints"] = 0
            result["validation"] = validation_time
            result["copy_elapsed"] = copy_elapsed
            result["n_jobs"] = len(partition_job.single_merge_jobs)
            self.log(
                "Was given an empty job..."
            )
            return result
        copy_elapsed += self.ingest_data(
            target_table, pd.concat(lps), observations
        )

        total_points = sum(len(lp) for lp in lps)
        result = dict(pd.DataFrame(timings).sum())
        result["relname"] = target_table
        result["n_lightpoints"] = total_points
        result["validation"] = validation_time
        result["copy_elapsed"] = copy_elapsed
        result["n_jobs"] = len(lightcurve_job.single_merge_jobs)

        return result


def ingest_merge_jobs(config, jobs, n_processes, commit, level_log="info", worker_class=None):
    """
    Process and ingest SingleMergeJob objects.
    """
    echo("Grabbing needed contexts from database")
    if worker_class is None:
        worker_class = PartitionConsumer

    with db_from_config(config) as db:
        cache = IngestionCache()
        echo("Loading spacecraft ephemeris")
        normalizer = TimeCorrector(db, cache)
        echo("Reading assigned quality flags")
        quality_flags = cache.quality_flag_map
        mid_tjd = db.get_mid_tjd_mapping()
        cache.session.close()

    echo("Building multiprocessing environment")
    workers = []
    manager = Manager()
    job_queue = manager.Queue()
    timing_queue = manager.Queue()
    total_single_jobs = 0

    for job in tqdm(jobs, unit=" jobs"):
        job_queue.put(job)
        total_single_jobs += len(job.single_merge_jobs)

    echo("Initializing workers, beginning processing...")
    with db_from_config(config) as db:
        for _ in range(n_processes):
            p = worker_class(
                db,
                quality_flags,
                mid_tjd,
                normalizer,
                job_queue,
                timing_queue,
                daemon=True,
            )
            job_queue.put(None)  # Kill sig
            p.start()

    logger.remove()
    with tqdm(total=total_single_jobs) as bar:
        logger.add(lambda msg: tqdm.write(msg, end=""), colorize=True, level="DEBUG", enqueue=True)

        # Wait until all jobs have been pulled off queue
        while not job_queue.empty():
            sleep(5)


    echo("Job queue empty, waiting for worker exits")
    for worker in workers:
        worker.join()

    echo("Joining work queues")
    job_queue.join()
    timing_queue.join()
