import multiprocessing as mp
import pathlib
from collections import defaultdict
from datetime import datetime
from os import getpid
from random import sample
from time import sleep

import h5py
import numpy as np
from loguru import logger
from pgcopy import CopyManager
from psycopg2.errors import InFailedSqlTransaction
from sqlalchemy import Integer, cast, func
from tqdm import tqdm

from lightcurvedb import models
from lightcurvedb.core.ingestors import em2_lightcurves as em2
from lightcurvedb.core.ingestors.consumer import BufferedDatabaseIngestor
from lightcurvedb.core.ingestors.correction import LightcurveCorrector

INGESTION_TELEMETRY_SLUG = "lightpoint-ingestion"
CSV_FIELDS = (
    "cadence",
    "barycentric_julian_date",
    "data",
    "error",
    "x_centroid",
    "y_centroid",
    "quality_flag",
)
CSV_TYPES = (
    int,  # cadence
    float,  # bjd
    float,  # data
    float,  # x_centroid
    float,  # y_centroid
    int,  # quality flag
)


def _to_csv(file_path, raw_data):
    field_iter = zip(*tuple(raw_data[field] for field in CSV_FIELDS))

    with open(file_path, "wt") as fout:
        for row in field_iter:
            fout.write(",".join(map(str, row)))
            fout.write("\n")


def _from_csv(file_path, lightcurve_id):
    with open(file_path, "r") as fin:
        for line in fin:
            tokens = line.split(",")
            parsed = tuple(
                cast(token) for cast, token in zip(CSV_TYPES, tokens)
            )
            yield lightcurve_id, *parsed


def _yield_from_csvs(files, lightcurves):
    for file, lightcurve in zip(files, lightcurves):
        yield from _from_csv(file, lightcurve.id)


class BaseEM2LightcurveIngestor(BufferedDatabaseIngestor):
    buffer_order = [
        "orbit_lightcurves",
        "best_orbit_lightcurves",
        "hyper_lightpoints",
    ]

    def __init__(
        self, config, name, job_queue, cache_path, lp_cache, result_queue=None
    ):

        super().__init__(config, name, job_queue)
        self.result_queue = result_queue
        self.apertures = {}
        self.lightcurve_types = {}
        self.bestap_cache = {}
        self.tmp_lc_id_map = {}
        self.runtime_parameters = {}
        self.current_sample = {}
        self.n_samples = 0
        self.process = None
        self.lp_cache = pathlib.Path(lp_cache)
        self.cache_path = cache_path

    def _load_contexts(self):
        with self.db as db:
            self.corrector = LightcurveCorrector(self.cache_path)
            self.orbit_map = dict(
                db.query(models.Orbit.orbit_number, models.Orbit.id)
            )
            stage = (
                self.db.query(models.QLPStage)
                .filter_by(slug=INGESTION_TELEMETRY_SLUG)
                .one()
            )
            self.stage_id = stage.id
            self.set_new_parameters(db)

    def _postflush(self, db):
        self.n_samples += 1

        if self.should_refresh_parameters:
            self.set_new_parameters(db)

    def get_best_aperture_id(self, tic_id):
        tmag = self.corrector.resolve_tic_parameters(tic_id, "tmag")
        magbins = np.array([6, 7, 8, 9, 10, 11, 12])
        bestaps = np.array([4, 3, 3, 2, 2, 2, 1])
        index = np.searchsorted(magbins, tmag)
        if index == 0:
            bestap = bestaps[0]
        elif index >= len(magbins):
            bestap = bestaps[-1]
        else:
            if tmag > magbins[index] - 0.5:
                bestap = bestaps[index]
            else:
                bestap = bestaps[index - 1]

        try:
            id_ = self.bestap_cache[bestap]
        except KeyError:
            self.log(f"Best Aperture cache miss for {bestap}, tmag of {tmag}")
            with self.db as db:
                id_ = db.resolve_best_aperture_id(bestap)
            self.bestap_cache[bestap] = id_

        return id_

    def get_best_detrend_id(self, h5_file):
        name = em2.best_detrending_from_h5_fd(h5_file)
        try:
            id_ = self.best_detrend_cache[name]
        except KeyError:
            self.log(f"Best Detrending cache miss for {name}, resolving")
            with self.db as db:
                id_ = db.resolve_beset_lightcurve_type_id(name)
            self.best_detrend_cache[name] = id_
        return id_

    def get_observed(self, tic_id):
        try:
            observed = self.observation_cache[tic_id]
        except KeyError:
            with self.db as db:
                lc = models.OrbitLightcurve
                o = models.Orbit
                ap = models.Aperture
                type_ = models.LightcurveType

                q = db.query(
                    lc.tic_id,
                    lc.camera,
                    lc.ccd,
                    o.orbit_number,
                    ap.name,
                    type_.name,
                ).filter(lc.tic_id == tic_id)
                observed = set(q.all())
                self.observation_cache[tic_id] = observed
        return observed

    def process_job(self, em2_h5_job):
        observed_mask = self.get_observed(em2_h5_job.tic_id)

        with h5py.File(em2_h5_job, "r") as h5:
            cadences = em2.get_cadences(h5)
            bjd = em2.get_barycentric_julian_dates(h5)

            mid_tjd = self.corrector.get_mid_tjd(em2_h5_job.camera, cadences)
            bjd = self.corrector.correct_for_earth_time(
                em2_h5_job.tic_id, mid_tjd
            )
            quality_flags = self.corrector.get_quality_flags(
                em2_h5_job.camera, em2_h5_job.ccd, cadences
            )
            bestap_id = self.get_best_aperture_id(em2_h5_job.tic_id)
            best_detrend_id = self.get_best_detrend_id(h5)
            best_lightcurve_definition = {
                "orbit_id": self.orbit_map[em2_h5_job.orbit_number],
                "aperture_id": bestap_id,
                "lightcurve_id": best_detrend_id,
                "tic_id": em2_h5_job.tic_id,
            }

            self.buffers["best_orbit_lightcurves"].append(
                best_lightcurve_definition
            )

            for ap_name, type_name, raw_data in em2.iterate_for_raw_data(h5):
                pos = len(self.buffers["orbit_lightcurve"])
                unique_key = (
                    em2_h5_job.tic_id,
                    em2_h5_job.camera,
                    em2_h5_job.ccd,
                    em2_h5_job.orbit_number,
                    ap_name,
                    type_name,
                )
                if unique_key in observed_mask:
                    continue

                raw_data["barycentric_julian_date"] = bjd
                raw_data["quality_flag"] = quality_flags

                aperture_id = self.get_aperture_id(ap_name)
                lightcurve_type_id = self.get_lightcurve_type_id(type_name)
                lightcurve = models.OrbitLightcurve(
                    tic_id=em2_h5_job.tic_id,
                    camera=em2_h5_job.camera,
                    ccd=em2_h5_job.ccd,
                    orbit_id=self.orbit_map[em2_h5_job.orbit_number],
                    aperture_id=aperture_id,
                    lightcurve_type_id=lightcurve_type_id,
                )

                lp_filepath = self.lp_cache / f"{pos}_{getpid()}_lp.csv"
                self.n_lightpoints += len(cadences)

                _to_csv(lp_filepath, raw_data)

                self.buffers["lightpoints"].append(lp_filepath)
                self.buffers["orbit_lightcurves"].append(lightcurve)

        self.job_queue.task_done()

    def flush_orbit_lightcurves(self, db):
        lightcurves = self.buffers["orbit_lightcurves"]
        self.log(f"Flushing {len(lightcurves):,} orbit lightcurves to remote")
        start = datetime.now()
        db.session.add_all(lightcurves)
        db.flush()
        end = datetime.now()

        metric = models.QLPOperation(
            process_id=self.process.id,
            time_start=start,
            time_end=end,
            job_size=len(lightcurves),
            unit="lightcurve",
        )
        return metric

    def flush_best_orbit_lightcurves(self, db):
        best_lcs = self.buffers["best_orbit_lightcurves"]
        self.log(
            f"Updating best lightcurve table with {len(best_lcs):,} entries"
        )
        start = datetime.now()
        db.session.bulk_insert_mapping(models.BestOrbitLightcurve, best_lcs)
        db.flush()
        end = datetime.now()

        metric = models.QLPOperation(
            process_id=self.process.id,
            time_start=start,
            time_end=end,
            job_size=len(best_lcs),
            unit="best_lightcurves",
        )
        return metric

    def flush_lightpoints(self, db):
        files = self.buffers["lightpoints"]
        lcs = self.buffers["orbit_lightcurves"]

        conn = db.session.connection().connection
        start = datetime.now()
        lp_size = self.n_lightpoints

        self.log(f"Flushing {lp_size:,} lightpoints to remote")

        try:
            mgr = CopyManager(
                conn,
                models.Lightpoint.__tablename__,
                models.Lightpoint.get_columns(),
            )

            mgr.threading_copy(_yield_from_csvs(files, lcs))
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                _healthcheck = cur.fetchall()  # noqa F841
        except InFailedSqlTransaction:
            # threading failed silently, raise here
            raise RuntimeError
        end = datetime.now()

        for f in files:
            f.unlink()

        self.n_lightpoints = 0

        metric = models.QLPOperation(
            process_id=self.process.id,
            time_start=start,
            time_end=end,
            job_size=lp_size,
            unit="lightpoint",
        )
        return metric

    def determine_process_parameters(self):
        raise NotImplementedError

    def set_new_parameters(self, db):
        self.log(f"Setting new parameters. DB State: {db}")
        process = models.QLPProcess(
            stage_id=self.stage_id,
            state="running",
            runtime_parameters=self.determine_process_parameters(),
        )
        db.add(process)
        db.session.flush()
        self.log(
            f"Updating runtime parameters to {process.runtime_parameters}"
        )
        if self.process is not None:
            q = db.query(models.QLPProcess).filter(
                models.QLPProcess.id == self.process.id
            )
            q.update({"state": "completed"}, synchronize_session=False)

        db.commit()
        self.process = process
        self.log(f"Added {self.process}")
        self.runtime_parameters = process.runtime_parameters

        self.n_samples = 0

    @property
    def should_flush(self):
        return (
            self.n_lightpoints
            >= self.runtime_parameters["lp_buffer_threshold"]
        )

    @property
    def should_refresh_parameters(self):
        return self.n_samples >= 3


class EM2BestParamSearchIngestor(BaseEM2LightcurveIngestor):
    step_size = 800
    max_steps = 62500

    def determine_process_parameters(self):
        step_col = cast(models.QLPOperation.job_size / self.step_size, Integer)

        q = (
            self.db.query(
                step_col.label("bucket"),
                func.count(models.QLPOperation.id),
            )
            .join(models.QLPOperation.process)
            .filter(models.QLPProcess.current_version())
            .group_by(step_col)
        )
        current_samples = dict(q)
        samples = defaultdict(list)

        for step in range(1, self.max_steps + 1):
            n_samples = current_samples.get(step, 0)
            samples[n_samples].append(step)
        lowest_sample_rate = min(samples.keys())
        possible_steps = samples[lowest_sample_rate]

        step = sample(possible_steps, 1)[0]

        return {"lp_buffer_threshold": self.step_size * step}


def _initialize_workers(WorkerClass, config, n_processes, **kwargs):
    workers = []
    logger.debug(f"Initializing {n_processes} workers")
    for n in range(n_processes):
        worker = WorkerClass(config, f"worker-{n:02}", **kwargs)
        worker.start()
        workers.append(worker)
    logger.debug(f"{n_processes} workers initialized and started")
    return workers


def ingest_jobs(db, jobs, n_processes, cache_path, lp_cache, log_level):
    manager = mp.Manager()
    job_queue = manager.Queue()

    workers = _initialize_workers(
        EM2BestParamSearchIngestor,
        db.config,
        n_processes,
        job_queue=job_queue,
        cache_path=cache_path,
        lp_cache=lp_cache,
    )

    with tqdm(total=len(jobs), unit=" jobs") as bar:
        logger.remove()
        logger.add(
            lambda msg: tqdm.write(msg, end=""),
            colorize=True,
            level=log_level.upper(),
            enqueue=True,
        )
        n_jobs_remaining = job_queue.qsize()
        while not job_queue.empty():
            current_qsize = job_queue.qsize()
            completed_since_last_check = n_jobs_remaining - current_qsize
            n_jobs_remaining = current_qsize
            bar.update(completed_since_last_check)
            sleep(1)

        job_queue.join()
    logger.debug("Waiting for workers to finish")
    for worker in workers:
        worker.join()
