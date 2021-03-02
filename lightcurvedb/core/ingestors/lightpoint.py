import os
import warnings
from collections import defaultdict, namedtuple
from functools import lru_cache
from itertools import product
from multiprocessing import Manager, Process
from time import time, sleep

import numpy as np
import pandas as pd
from click import echo
from pgcopy import CopyManager
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from tqdm import tqdm

from lightcurvedb import db_from_config
from lightcurvedb.core.ingestors.cache import IngestionCache
from lightcurvedb.core.ingestors.jobs import IngestionPlan, SingleMergeJob
from lightcurvedb.core.ingestors.lightcurve_ingestors import (
    allocate_lightcurve_ids,
    get_components,
    get_h5,
    get_missing_ids,
    load_lightpoints,
)
from lightcurvedb.core.ingestors.temp_table import FileObservation
from lightcurvedb.legacy.timecorrect import TimeCorrector
from lightcurvedb.models import (
    Aperture,
    Frame,
    Lightcurve,
    LightcurveType,
    Lightpoint,
    Observation,
    Orbit,
)
from lightcurvedb.util.logger import lcdb_logger as logger

LC_ERROR_TYPES = {"RawMagnitude"}


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
        with db_from_config() as db:
            conn = db.session.connection().connection
            with conn.cursor() as cursor:
                cursor.execute("SET LOCAL work_mem TO '2GB'")
                cursor.execute("SET LOCAL synchronous_commit = OFF")

            mgr = CopyManager(
                conn, partition_relname, Lightpoint.get_columns()
            )

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
        return copy_elapsed

    def process_job(self, partition_job):
        lps = []
        observations = []
        timings = []
        validation_time = 0
        total_points = 0
        copy_elapsed = 0

        optimized_path = sorted(
            partition_job.single_merge_jobs,
            key=lambda job: (job.lightcurve_id, job.orbit_number),
        )

        for lc_job in optimized_path:
            lp, timing = self.process_h5(
                lc_job.lightcurve_id,
                lc_job.aperture,
                lc_job.lightcurve_type,
                lc_job.file_path,
            )
            timings.append(timing)

            stamp = time()
            lp.set_index(["lightcurve_id", "cadence"], inplace=True)
            lp = lp[~lp.index.duplicated(keep="last")]
            validation_time += time() - stamp

            if not lp.empty:
                orbit_id = self.orbit_map[lc_job.orbit_number]
                observations = [
                    (
                        lc_job.lightcurve_id,
                        orbit_id,
                        lc_job.camera,
                        lc_job.ccd,
                    )
                ]

                copy_elapsed += self.ingest_data(
                    partition_job.partition_relname, lp, observations
                )
                total_points += len(lp)

        result = dict(pd.DataFrame(timings).sum())
        result["relname"] = partition_job.partition_relname
        result["n_lightpoints"] = total_points
        result["validation"] = validation_time
        result["copy_elapsed"] = copy_elapsed

        return result

    def run(self):
        self.set_name()
        self.cached_get_qflags = lru_cache(maxsize=16)(self.get_qflags)
        self.cached_get_correct_mid_tjd = lru_cache(maxsize=16)(
            self.get_correct_mid_tjd
        )

        job = self.job_queue.get()
        timeout = 1
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore", r"All-NaN (slice|axis) encountered"
            )
            while job is not None:
                for _ in range(20):
                    try:
                        results = self.process_job(job)
                        break
                    except OperationalError:
                        sleep(timeout)
                        timeout *= 2
                        continue
                    self.job_queue.task_done()
                    raise RuntimeError(
                        "Database too busy, worker exiting prematurely"
                    )

                self.result_queue.put(results)
                self.job_queue.task_done()
                job = self.job_queue.get()

        self.job_queue.task_done()
        self.log("Recieved end-of-work signal")


def get_merge_jobs(ctx, cache, orbits, cameras, ccds, fillgaps=False):
    """
    Get a list of SingleMergeJobs from TICs appearing in the
    given orbits, cameras, and ccds. TICs and orbits already ingested
    will *not* be in the returned list.

    In addition, any new Lightcurves will be assigned IDs so it will
    be deterministic in which partition its data will reside in.

    Parameters
    ----------
    ctx : click.Context
        A click context object which should contain an unopened
        lightcurvedb.DB object
    cache : IngestionCache
        An opened ingestion cache object
    orbits : sequence of int
        The desired orbits to ingest.
    cameras : sequence of int
        The cameras to ingest
    ccds : sequence of int
        The ccds to ingest
    fillgaps : bool, optional
        If true, find gaps in the lightcurve id sequence and fill them with
        any new lightcurves.
    Returns
    -------
    sequence of lightcurve.core.ingestors.lightpoint.SingleMergeJob
        Return a sequence of NamedTuples describing needed
        information for ingest.
    """
    cache_q = cache.session.query(
        FileObservation.tic_id,
        FileObservation.orbit_number,
        FileObservation.file_path,
    )

    if not all(cam in cameras for cam in [1, 2, 3, 4]):
        cache_q = cache_q.filter(FileObservation.camera.in_(cameras))
    if not all(ccd in ccds for ccd in [1, 2, 3, 4]):
        cache_q = cache_q.filter(FileObservation.ccd.in_(ccds))

    file_df = pd.read_sql(
        cache_q.statement, cache.session.bind, index_col=["tic_id"]
    )

    relevant_tics = set(file_df[file_df.orbit_number.isin(orbits)].index)

    file_df = file_df.loc[relevant_tics].sort_index()

    obs_clause = (
        Orbit.orbit_number.in_(orbits),
        Observation.camera.in_(cameras),
        Observation.ccd.in_(ccds),
    )
    echo("Comparing cache file paths to lcdb observation table")
    with ctx.obj["dbconf"] as db:
        sub_obs_q = (
            db.query(Observation.tic_id)
            .join(Observation.orbit)
            .filter(*obs_clause)
        )

        obs_q = (
            db.query(Observation.tic_id, Orbit.orbit_number)
            .join(Observation.orbit)
            .filter(Observation.tic_id.in_(sub_obs_q.subquery()))
        )
        apertures = [ap.name for ap in db.query(Aperture)]
        types = [t.name for t in db.query(LightcurveType)]

        echo("Determining existing observations")
        already_observed = set(obs_q)

        echo("Preparing lightcurve id map")
        lcs = db.lightcurves.filter(Lightcurve.tic_id.in_(relevant_tics))
        lc_id_map = {
            (lc.tic_id, lc.aperture_id, lc.lightcurve_type_id): lc.id
            for lc in lcs.yield_per(10000)
        }
        jobs = list(
            get_jobs(
                db,
                file_df,
                lc_id_map,
                already_observed,
                apertures,
                types,
                fill_id_gaps=fillgaps,
                bar=tqdm,
            )
        )
    return jobs


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
            Frame.camera, Frame.cadence, Frame.mid_tjd,
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

    for job in jobs:
        job_queue.put(job)

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

    with tqdm(range(len(jobs))) as bar:
        for _ in range(len(jobs)):
            timings = timing_queue.get()
            msg = (
                "{relname}: {n_lightpoints} | "
                "File Load: {file_load:3.2f}s | "
                "QFlag Load: {quality_flag_assignment:3.2f}s | "
                "BJD Load: {bjd_correction:3.2f}s | "
                "Validate: {validation:3.2f}s | "
                "COPY TIME: {copy_elapsed:3.2f}s"
            )
            bar.write(msg.format(**timings))

            timing_queue.task_done()
            bar.update(1)

    echo("Expected number of returns reached, joining processes...")
    for worker in workers:
        worker.join()

    echo("Joining work queues")
    job_queue.join()
    timing_queue.join()
    echo("Done!")
