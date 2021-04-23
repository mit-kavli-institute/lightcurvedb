import os
import warnings
from functools import lru_cache
from sqlalchemy import text
from multiprocessing import Manager, Process
from time import time

import numpy as np
import pandas as pd
from click import echo
from pgcopy import CopyManager
from sqlalchemy import text
from tqdm import tqdm

from lightcurvedb import db_from_config
from lightcurvedb.core.engines import psycopg_connection
from lightcurvedb.core.ingestors.cache import IngestionCache
from lightcurvedb.core.ingestors.lightcurve_ingestors import (
    get_h5,
    get_h5_data,
    get_components,
    get_correct_qflags,
    get_lightcurve_median,
    get_tjd,
    get_components
)
from lightcurvedb.legacy.timecorrect import TimeCorrector
from lightcurvedb.models import (
    Frame,
    Lightpoint,
    Observation,
    Orbit,
)
from lightcurvedb.util.logger import lcdb_logger as logger
from lightcurvedb.util.decorators import track_runtime

LC_ERROR_TYPES = {"RawMagnitude"}


def yield_lp_kwarg_from_merge_jobs(normalizer, stellar_params, merge_job, config_override=None):
    tmag = stellar_params.loc[merge_job.tic_id]["tmag"]
    # Load in h5 data
    lp_raw_kwargs, file_time = get_h5_data(merge_job)

    # Load in manual quality flags
    qflags, qflag_time = get_correct_qflags(merge_job, lp_raw_kwargs["cadence"])

    # Align orbit data to tmag median
    aligned_data, align_time = get_lightcurve_median(
        lp_raw_kwargs["data"],
        qflags,
        tmag
    )

    # Pull tjd data from Frame table and correct for earth time
    tjd, tjd_pull_time = get_tjd(merge_job, lp_raw_kwargs["cadence"], config_override=config_override)
    correct_bjd = normalizer.correct(merge_job.tic_id, tjd)

    lp_raw_kwargs["data"] = aligned_data
    lp_raw_kwargs["barycentric_julian_date"] = correct_bjd
    lp_raw_kwargs["quality_flag"] = qflags
    lp_raw_kwargs["lightcurve_id"] = np.full_like(lp_raw_kwargs["cadence"], merge_job.lightcurve_id, dtype=int)

    timings = {
            "file_load": file_time,
            "quality_flag_assignment": qflag_time,
            "bjd_correction": tjd_pull_time,
            "alignment": align_time,
    }

    return lp_raw_kwargs, timings



def yield_lp_df_from_merge_jobs(normalizer, stellar_params, single_merge_jobs, config_override=None):
    for job in single_merge_jobs:
        lp_raw_kwargs, timings = yield_lp_kwarg_from_merge_jobs(
            normalizer,
            stellar_params,
            job,
            config_override=config_override
        )

        lp = pd.DataFrame(
            data=lp_raw_kwargs
        )

        yield lp, timings


class LightpointProcessor(Process):
    def log(self, msg, level="debug", exc_info=False):
        getattr(logger, level)(
            "{0}: {1}".format(self.name, msg), exc_info=exc_info
        )

    def set_name(self):
        self.name = "{0}-{1}".format(self.prefix, os.getpid())


class PartitionConsumer(LightpointProcessor):
    prefix = "Copier"

    def __init__(
        self,
        db,
        qflags,
        mid_tjd,
        normalizer,
        job_queue,
        result_queue,
        **kwargs
    ):
        super(PartitionConsumer, self).__init__(**kwargs)

        self.orbit_map = dict(db.query(Orbit.orbit_number, Orbit.id))
        self.job_queue = job_queue
        self.result_queue = result_queue
        self.corrector = normalizer
        self.qflags = qflags
        self.mid_tjd = mid_tjd

        self.cached_get_qflags = None
        self.cached_get_correct_mid_tjd = None

    def get_qflags(self, h5path):
        cadences = get_h5(h5path)["LightCurve"]["Cadence"][()].astype(int)
        context = get_components(h5path)
        camera, ccd = context["camera"], context["ccd"]
        sqflag = self.qflags.loc[(camera, ccd)].loc[cadences]

        return sqflag.quality_flag.to_numpy()

    def get_correct_mid_tjd(self, h5path):
        cadences = get_h5(h5path)["LightCurve"]["Cadence"][()].astype(int)
        context = get_components(h5path)
        tic_id, camera = (
            context["tic_id"],
            context["camera"],
        )
        mid_tjd = self.mid_tjd.loc[camera].loc[cadences].mid_tjd.to_numpy()

        return self.corrector.correct(tic_id, mid_tjd)

    def process_h5(self, id_, aperture, lightcurve_type, h5path):
        timings = {}
        stamp = time()

        context = get_components(h5path)
        tmag = self.corrector.tic_parameters.loc[context["tic_id"]]["tmag"]
        h5in = get_h5(h5path)
        h5_lc = h5in["LightCurve"]
        cadences = h5_lc["Cadence"][()].astype(int)

        h5_lc = h5_lc["AperturePhotometry"][aperture]
        x_centroids = h5_lc["X"][()]
        y_centroids = h5_lc["Y"][()]
        data = h5_lc[lightcurve_type][()]

        errors = (
            h5_lc["{0}Error".format(lightcurve_type)][()]
            if lightcurve_type in LC_ERROR_TYPES
            else np.full_like(cadences, np.nan, dtype=np.double)
        )

        timings["file_load"] = time() - stamp

        stamp = time()
        qflags = self.cached_get_qflags(h5path)
        timings["quality_flag_assignment"] = time() - stamp
        stamp = time()

        correct_mid_tjd = self.cached_get_correct_mid_tjd(h5path)
        timings["bjd_correction"] = time() - stamp

        lp = pd.DataFrame(
            data={
                "cadence": cadences,
                "barycentric_julian_date": correct_mid_tjd,
                "data": data,
                "error": errors,
                "x_centroid": x_centroids,
                "y_centroid": y_centroids,
                "quality_flag": qflags,
            }
        )
        lp["lightcurve_id"] = id_

        stamp = time()
        mask = lp.quality_flag == 0
        good_values = lp[mask].data.to_numpy()
        offset = np.nanmedian(good_values) - tmag
        lp.data = lp.data - offset
        timings["alignment"] = time() - stamp

        return lp, timings

    def ingest_data(self, partition_relname, lightpoints, observations):
        conn = psycopg_connection()
        with conn.cursor() as cursor:
            cursor.execute("SET LOCAL work_mem TO '2GB'")
            cursor.execute("SET LOCAL synchronous_commit = OFF")

        mgr = CopyManager(conn, partition_relname, Lightpoint.get_columns())

        stamp = time()
        mgr.threading_copy(lightpoints.to_records())

        mgr = CopyManager(
            conn,
            Observation.__tablename__,
            ["lightcurve_id", "orbit_id", "camera", "ccd"],
        )

        mgr.threading_copy(observations)

        copy_elapsed = time() - stamp

        conn.commit()
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
            q = text(
                """
                SELECT nsp.nspname, pgc.relname
                FROM pg_class pgc
                JOIN pg_namespace nsp
                    ON pgc.relnamespace = nsp.oid
                WHERE pgc.oid = {0}
                """
                .format(
                    partition_job.partition_oid
                )
            )
            try:
                results = list(db.execute(q))
                namespace, tablename = results[0]
            except IndexError:
                raise RuntimeError(
                    "Could not find table for partition oid {0}".format(
                        partition_job.partition_oid
                    )
                )

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
                self.log("Unable to open {0}".format(lc_job.file_path), level="error")
                continue
            timings.append(timing)

            stamp = time()
            lp.set_index(["lightcurve_id", "cadence"], inplace=True)
            lp = lp[~lp.index.duplicated(keep="last")]
            validation_time += time() - stamp

            if not lp.empty:
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
            target_table, pd.concat(lps), observations
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
        self.cached_get_qflags = lru_cache(maxsize=16)(self.get_qflags)
        self.cached_get_correct_mid_tjd = lru_cache(maxsize=16)(
            self.get_correct_mid_tjd
        )

        job = self.job_queue.get()
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
        self.log("Recieved end-of-work signal")


def ingest_merge_jobs(config, jobs, n_processes, commit, tqdm_bar=True):
    """
    Group single
    """
    echo("Grabbing needed contexts from database")
    with db_from_config(config) as db:
        cache = IngestionCache()
        echo("Loading spacecraft ephemeris")
        normalizer = TimeCorrector(db, cache)
        echo("Reading assigned quality flags")
        quality_flags = cache.quality_flag_df
        mid_tjd_q = db.query(
            Frame.camera,
            Frame.cadence,
            Frame.mid_tjd,
        ).filter(Frame.frame_type_id == "Raw FFI")
        echo("Getting Raw FFI Mid TJD arrays")
        mid_tjd = pd.read_sql(
            mid_tjd_q.statement, db.bind, index_col=["camera", "cadence"]
        ).sort_index()

        cache.session.close()

    echo("Building multiprocessing environment")
    workers = []
    manager = Manager()
    job_queue = manager.Queue()
    timing_queue = manager.Queue()
    total_single_jobs = 0

    for job in jobs:
        job_queue.put(job)
        total_single_jobs += len(job.single_merge_jobs)

    echo("Initializing workers, beginning processing...")
    with db_from_config(config) as db:
        for _ in range(n_processes):
            p = PartitionConsumer(
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

    with tqdm(total=total_single_jobs) as bar:
        for _ in range(len(jobs)):
            timings = timing_queue.get()
            total_time = (
                timings["file_load"] +
                timings["bjd_correction"] +
                timings["validation"] +
                timings["copy_elapsed"]
            )
            timings["lightpoint_rate"] = timings["n_lightpoints"] / total_time

            msg = (
                "{relname}: {lightpoint_rate:5.2f} lp/s | "
                "File Load: {file_load:3.2f}s | "
                "QFlag Load: {quality_flag_assignment:3.2f}s | "
                "BJD Load: {bjd_correction:3.2f}s | "
                "COPY TIME: {copy_elapsed:3.2f}s | "
                "# of Jobs: {n_jobs}s"
            )
            bar.write(msg.format(**timings))

            timing_queue.task_done()
            bar.update(timings["n_jobs"])

    echo("Expected number of returns reached, joining processes...")
    for worker in workers:
        worker.join()

    echo("Joining work queues")
    job_queue.join()
    timing_queue.join()
    echo("Done!")
