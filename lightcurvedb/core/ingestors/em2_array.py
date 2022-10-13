import multiprocessing as mp
from collections import defaultdict, namedtuple
from datetime import datetime
from random import sample
from time import sleep

import cachetools
import h5py
import numpy as np
from loguru import logger
from pgcopy import CopyManager
from sqlalchemy import Integer, cast, func
from tqdm import tqdm

from lightcurvedb import models
from lightcurvedb.core.ingestors import em2_lightcurves as em2
from lightcurvedb.core.ingestors.consumer import BufferedDatabaseIngestor
from lightcurvedb.core.ingestors.correction import LightcurveCorrector

INGESTION_TELEMETRY_SLUG = "lightpoint-ingestion"
INGESTION_COLS = (
    "tic_id",
    "camera",
    "ccd",
    "orbit_id",
    "aperture_id",
    "lightcurve_type_id",
    "cadences",
    "barycentric_julian_dates",
    "data",
    "errors",
    "x_centroids",
    "y_centroids",
    "quality_flags",
)

ArrayLCPayload = namedtuple("ArrayLCPayload", INGESTION_COLS)


def _nan_compat(array):
    compat_arr = []
    for elem in array:
        if np.isnan(elem):
            compat_arr.append("NaN")
        else:
            compat_arr.append(elem)
    return compat_arr


class BaseEM2ArrayIngestor(BufferedDatabaseIngestor):
    buffer_order = [models.ArrayOrbitLightcurve.__tablename__]

    def __init__(self, config, name, job_queue, cache_path):
        super().__init__(config, name, job_queue)
        self.aperture_cache = cachetools.FIFOCache(8)
        self.lightcurve_type_cache = cachetools.FIFOCache(8)
        self.observation_cache = cachetools.FIFOCache(32)
        self.runtime_parameters = {}
        self.current_sample = {}
        self.n_samples = 0
        self.process = None
        self.cache_path = cache_path

    def _load_contexts(self):
        with self.db as db:
            self.corrector = LightcurveCorrector(self.cache_path)
            self.orbit_map = dict(
                db.query(models.Orbit.orbit_number, models.Orbit.id)
            )
            stage = (
                db.query(models.QLPStage)
                .filter(models.QLPStage.slug == INGESTION_TELEMETRY_SLUG)
                .one()
            )
            self.stage_id = stage.id
            self.set_new_parameters(db)

    def _postflush(self, db):
        self.n_samples += 1
        if self.should_refresh_parameters:
            self.set_new_parameters(db)

    def get_observed(self, tic_id):
        try:
            observed = self.observation_cache[tic_id]
        except KeyError:
            with self.db as db:
                lc = models.ArrayOrbitLightcurve
                o = models.Orbit
                ap = models.Aperture
                type_ = models.LightcurveType

                q = (
                    db.query(
                        lc.tic_id,
                        lc.camera,
                        lc.ccd,
                        o.orbit_number,
                        ap.name,
                        type_.name,
                    )
                    .join(lc.orbit)
                    .join(lc.aperture)
                    .join(lc.lightcurve_type)
                    .filter(lc.tic_id == tic_id)
                )
                observed = set(q.all())
                self.observation_cache[tic_id] = observed
        return observed

    def _resolve_id_from_name(self, Model, name):
        with self.db as db:
            q = db.query(Model.id).filter(Model.name == name)
            id_ = q.first()[0]

        self.log(
            f"Resolved '{name}' to numeric identifier {id_}", level="debug"
        )
        return id_

    def get_aperture_id(self, name):
        try:
            id_ = self.aperture_cache[name]
        except KeyError:
            id_ = self._resolve_id_from_name(models.Aperture, name)
            self.aperture_cache[name] = id_
        return id_

    def get_lightcurve_type_id(self, name):
        try:
            id_ = self.lightcurve_type_cache[name]
        except KeyError:
            id_ = self._resolve_id_from_name(models.LightcurveType, name)
            self.lightcurve_type_cache[name] = id_
        return id_

    def process_job(self, em2_h5_job):
        observed_mask = self.get_observed(em2_h5_job.tic_id)

        with h5py.File(em2_h5_job.file_path, "r") as h5:
            cadences = em2.get_cadences(h5)
            bjd = em2.get_barycentric_julian_dates(h5)

            mid_tjd = self.corrector.get_mid_tjd(em2_h5_job.camera, cadences)
            bjd = self.corrector.correct_for_earth_time(
                em2_h5_job.tic_id, mid_tjd
            )
            quality_flags = self.corrector.get_quality_flags(
                em2_h5_job.camera, em2_h5_job.ccd, cadences
            )

            for ap_name, type_name, raw_data in em2.iterate_for_raw_data(h5):
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
                lightcurve = ArrayLCPayload(
                    tic_id=em2_h5_job.tic_id,
                    camera=em2_h5_job.camera,
                    ccd=em2_h5_job.ccd,
                    orbit_id=self.orbit_map[em2_h5_job.orbit_number],
                    aperture_id=aperture_id,
                    lightcurve_type_id=lightcurve_type_id,
                    cadences=list(raw_data["cadence"]),
                    barycentric_julian_dates=list(
                        raw_data["barycentric_julian_date"]
                    ),
                    data=list(raw_data["data"]),
                    errors=list(raw_data["error"]),
                    x_centroids=list(raw_data["x_centroid"]),
                    y_centroids=list(raw_data["y_centroid"]),
                    quality_flags=list(raw_data["quality_flag"]),
                )
                buffer = self.buffers[
                    models.ArrayOrbitLightcurve.__tablename__
                ]
                buffer.append(lightcurve)
        self.job_queue.task_done()

    def flush_array_orbit_lightcurves(self, db):
        lightcurves = self.buffers[models.ArrayOrbitLightcurve.__tablename__]
        self.log(f"Flushing {len(lightcurves):,} lightcurves")
        start = datetime.now()
        mgr = CopyManager(
            db.session.connection().connection,
            models.ArrayOrbitLightcurve.__tablename__,
            INGESTION_COLS,
        )
        mgr.threading_copy(lightcurves)
        end = datetime.now()

        metric = models.QLPOperation(
            process_id=self.process.id,
            time_start=start,
            time_end=end,
            job_size=len(lightcurves),
            unit="array_lightcurves",
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
            len(self.buffers[models.ArrayOrbitLightcurve.__tablename__])
            >= self.runtime_parameters["n_lightcurves"]
        )

    @property
    def should_refresh_parameters(self):
        return self.n_samples >= 3


class EM2ArrayParamSearchIngestor(BaseEM2ArrayIngestor):
    step_size = 10
    max_steps = 10000

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

        return {"n_lightcurves": self.step_size * step}


def _initialize_workers(WorkerClass, config, n_processes, **kwargs):
    workers = []
    logger.debug(f"Initializing {n_processes} workers")
    for n in range(n_processes):
        worker = WorkerClass(config, f"worker-{n:02}", **kwargs)
        worker.start()
        workers.append(worker)
    logger.debug(f"{n_processes} workers initialized and started")
    return workers


def ingest_jobs(db, jobs, n_processes, cache_path, log_level):
    manager = mp.Manager()
    job_queue = manager.Queue()

    with tqdm(total=len(jobs), unit=" jobs") as bar:
        logger.remove()
        logger.add(
            lambda msg: tqdm.write(msg, end=""),
            colorize=True,
            level=log_level.upper(),
            enqueue=True,
        )
        workers = _initialize_workers(
            EM2ArrayParamSearchIngestor,
            db.config,
            n_processes,
            job_queue=job_queue,
            cache_path=cache_path,
        )

        for job in jobs:
            job_queue.put(job)

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
