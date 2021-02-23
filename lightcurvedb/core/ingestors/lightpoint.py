import os
import warnings
from collections import defaultdict, namedtuple
from functools import lru_cache
from itertools import product
from multiprocessing import Manager, Process
from time import time

import numpy as np
import pandas as pd
from click import echo
from pgcopy import CopyManager
from sqlalchemy import text
from tqdm import tqdm

from lightcurvedb import db_from_config
from lightcurvedb.core.ingestors.cache import IngestionCache
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

SingleMergeJob = namedtuple(
    "SingleMergeJob", ("tic_id", "aperture", "lightcurve_type", "id", "files")
)

PartitionJob = namedtuple(
    "PartitionJob", ("partition_relname", "single_merge_jobs")
)


def get_jobs(
    db,
    file_df,
    id_map,
    already_observed,
    apertures,
    types,
    fill_id_gaps=False,
    bar=None,
):
    """
    Return single merge jobs from a observation file dataframe and
    a list of apertures and lightcurve types.
    """
    missing_ids = set()

    file_df = file_df.reset_index().set_index(["tic_id", "orbit_number"])
    new_file_df = file_df[~file_df.index.isin(already_observed)]

    print("Removed {0} duplicate jobs".format(len(file_df) - len(new_file_df)))

    if bar:
        progress = bar(
            total=len(new_file_df.reset_index().tic_id.unique())
            * len(apertures)
            * len(types)
        )

    for tic_id, subdf in new_file_df.reset_index().groupby("tic_id"):
        files = list(subdf.file_path)
        for ap, lc_t in product(apertures, types):
            try:
                id_ = id_map[(tic_id, ap, lc_t)]
                job = SingleMergeJob(
                    id=id_,
                    tic_id=tic_id,
                    aperture=ap,
                    lightcurve_type=lc_t,
                    files=files,
                )
                yield job
            except KeyError:
                missing_ids.add((tic_id, ap, lc_t))
            finally:
                if bar:
                    progress.update(1)

    if bar:
        progress.close()

    if missing_ids:
        echo(
            (
                "Need to assign {0} "
                "new lightcurve ids".format(len(missing_ids))
            )
        )
        n_required = len(missing_ids)
        params = iter(missing_ids)
        if fill_id_gaps:
            echo("Attempting to find gaps in id sequence")
            usable_ids = get_missing_ids(db)
            echo("Found {0} ids to fill".format(len(usable_ids)))
        else:
            usable_ids = set()

        n_still_missing = n_required - len(usable_ids)
        usable_ids.update(allocate_lightcurve_ids(db, n_still_missing))
        values_to_insert = []
        echo("Creating jobs using queried ids")
        progress = bar(total=len(missing_ids))
        for id_, params in zip(usable_ids, missing_ids):
            files = list(file_df.loc[params[0]].file_path)
            job = SingleMergeJob(
                id=id_,
                tic_id=params[0],
                aperture=params[1],
                lightcurve_type=params[2],
                files=files,
            )
            values_to_insert.append(
                {
                    "id": id_,
                    "tic_id": job.tic_id,
                    "aperture_id": job.aperture,
                    "lightcurve_type_id": job.lightcurve_type,
                }
            )
            yield job
            progress.update(1)

        progress.close()
        # update db
        echo("Submitting new lightcurve parameters to database")
        db.session.bulk_insert_mappings(Lightcurve, values_to_insert)
        db.commit()


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

    def process_job(self, job):
        lightcurve_jobs = sorted(job.single_merge_jobs, key=lambda j: j.id)
        lps = []
        observations = []
        timings = []

        for lc_job in lightcurve_jobs:
            for h5file in lc_job.files:
                context = get_components(h5file)
                lp, timing = self.process_h5(
                    lc_job.id, lc_job.aperture, lc_job.lightcurve_type, h5file
                )
                timings.append(timing)
                if len(lp) > 0:
                    orbit_id = self.orbit_map[context["orbit_number"]]
                    observations.append(
                        {
                            "tic_id": lc_job.tic_id,
                            "orbit_id": orbit_id,
                            "camera": context["camera"],
                            "ccd": context["ccd"],
                        }
                    )
                    lps.append(lp)

        result = dict(pd.DataFrame(timings).sum())
        result["relname"] = job.partition_relname

        stamp = time()
        partition_df = pd.concat(lps)
        partition_df.set_index(["lightcurve_id", "cadence"], inplace=True)

        result["n_lightpoints"] = len(partition_df)

        observation_df = pd.DataFrame(observations).set_index(
            ["tic_id", "orbit_id"]
        )
        observation_df.sort_index(inplace=True)
        observation_df = observation_df[
            ~observation_df.index.duplicated(keep="last")
        ].reset_index()

        result["validation"] = time() - stamp

        with db_from_config() as db:
            mem_q = text("SET LOCAL work_mem TO '2GB'")
            async_q = text("SET LOCAL synchronous_commit = OFF")
            db.session.execute(mem_q)
            db.session.execute(async_q)

            conn = db.session.connection().connection
            mgr = CopyManager(
                conn, job.partition_relname, Lightpoint.get_columns()
            )

            stamp = time()
            mgr.threading_copy(partition_df.to_records())

            q = Observation.upsert_q()
            db.session.execute(q, observation_df.to_dict("records"))

            result["copy_elapsed"] = time() - stamp

            conn.commit()
            db.commit()
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


def copy_lightpoints(config, corrector, merge_jobs, commit=True):
    lps = []
    start = time()
    n_files = 0
    missed_filepaths = []
    seen_obs = set()
    obs = []
    for merge_job in merge_jobs:
        for h5 in merge_job.files:
            try:
                context = get_components(h5)
                lp = load_lightpoints(
                    h5,
                    merge_job.id,
                    merge_job.aperture,
                    merge_job.lightcurve_type,
                )
                lp = corrector.correct(merge_job.tic_id, lp)
                lps.append(lp)

                orbit = int(context["orbit_number"])
                orbit_id = corrector.orbit_map[orbit]
                key = (merge_job.tic_id, orbit_id)
                if key not in seen_obs:
                    camera = int(context["camera"])
                    ccd = int(context["ccd"])
                    obs.append(
                        {
                            "tic_id": merge_job.tic_id,
                            "orbit_id": orbit_id,
                            "camera": camera,
                            "ccd": ccd,
                        }
                    )
                    seen_obs.add(key)
                n_files += 1
            except (RuntimeError, OSError):
                missed_filepaths.append(h5)
                continue

    merge_elapsed = time() - start

    if not lps:
        return {
            "status": "ERROR",
            "n_files": n_files,
            "missed_files": missed_filepaths,
        }

    # Establish full datastructures for partition and observation
    # updates
    start = time()
    raw_partition = pd.concat(lps)
    raw_partition["orbit_id"] = raw_partition.apply(
        lambda row: corrector.orbit_map[row["orbit_number"]], axis=1
    )
    raw_partition.drop(columns="orbit_number", inplace=True)

    lp = raw_partition[list(Lightpoint.get_columns())]
    lp = lp.set_index(["lightcurve_id", "cadence"])
    lp.sort_index(inplace=True)

    # Remove any duplication
    lp = lp[~raw_partition.index.duplicated(keep="last")]

    validation_elapsed = time() - start

    # Establish database connection
    with db_from_config(config) as db:
        # Set worker memory
        mem_q = text("SET LOCAL work_mem TO '1GB'")
        db.session.execute(mem_q)

        conn = db.session.connection().connection

        mgr = CopyManager(
            conn, Lightpoint.__tablename__, Lightpoint.get_columns()
        )

        start = time()
        mgr.threading_copy(lp.to_records())

        conn.commit()

        copy_elapsed = time() - start

        q = Observation.upsert_q()

        start = time()
        db.session.execute(q, obs)

        upsert_elapsed = time() - start
        start = time()
        db.commit()
        commit_elapsed = time() - start

    return {
        "status": "OK",
        "n_files": n_files,
        "missed_files": missed_filepaths,
        "merge_elapsed": merge_elapsed,
        "validation_elapsed": validation_elapsed,
        "copy_elapsed": copy_elapsed,
        "upsert_elapsed": upsert_elapsed,
        "commit_elapsed": commit_elapsed,
    }


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


def get_jobs_by_tic(
    ctx, cache, tics, fillgaps=False, orbits=None, apertures=None, types=None
):
    cache_q = cache.session.query(
        FileObservation.tic_id,
        FileObservation.orbit_number,
        FileObservation.file_path,
    ).filter(FileObservation.tic_id.in_(tics))
    if orbits:
        cache_q = cache_q.filter(Fileobservation.orbit_number.in_(orbits))

    file_df = pd.read_sql(
        cache_q.statement, cache.session.bind, index_col=["tic_id"]
    ).sort_index()

    echo("Comparing cache file paths to lcdb observation table")
    with ctx.obj["dbconf"] as db:
        obs_q = (
            db.query(Observation.tic_id, Orbit.orbit_number)
            .join(Observation.orbit)
            .filter(Observation.tic_id.in_(tics))
        )
        apertures = (
            [ap.name for ap in db.query(Aperture)]
            if not apertures
            else apertures
        )
        types = (
            [t.name for t in db.query(LightcurveType)] if not types else types
        )

        echo("Determining existing observations")
        already_observed = set(obs_q)

        echo("Preparing lightcurve id map")
        lcs = db.lightcurves.filter(Lightcurve.tic_id.in_(tics))
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


def ingest_merge_jobs(config, merge_jobs, n_processes, commit, tqdm_bar=True):
    """
    Group single
    """
    # Group each merge_job
    bucket = defaultdict(list)
    for merge_job in merge_jobs:
        partition_id = (merge_job.id // 1000) * 1000
        bucket[partition_id].append(merge_job)

    jobs = []

    for k, joblist in bucket.items():
        relname = "partitions.lightpoints_{0}_{1}".format(k, k + 1000)

        jobs.append(
            PartitionJob(partition_relname=relname, single_merge_jobs=joblist)
        )

    echo("{0} partitions will be affected".format(len(jobs)))

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
