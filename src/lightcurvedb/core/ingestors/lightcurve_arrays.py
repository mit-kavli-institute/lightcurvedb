import multiprocessing as mp
from collections import defaultdict
from datetime import datetime
from random import sample
from time import sleep, time

import cachetools
import h5py
from loguru import logger
from sqlalchemy import Integer, cast, func
from sqlalchemy.dialects.postgresql import insert as psql_insert
from tqdm import tqdm

from lightcurvedb import models
from lightcurvedb.core.connection import DB
from lightcurvedb.core.ingestors import lightcurves as em2
from lightcurvedb.core.ingestors.consumer import BufferedDatabaseIngestor
from lightcurvedb.core.ingestors.correction import LightcurveCorrector
from lightcurvedb.models.lightcurve import ArrayOrbitLightcurve

INGESTION_TELEMETRY_SLUG = "lightpoint-ingestion"


class BaseEM2ArrayIngestor(BufferedDatabaseIngestor):
    buffer_order = [
        models.ArrayOrbitLightcurve.__tablename__,
        models.BestOrbitLightcurve.__tablename__,
    ]

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

    def get_best_aperture_id(self, h5):
        bestap = int(h5["LightCurve"]["AperturePhotometry"].attrs["bestap"])
        name = f"Aperture_{bestap:03d}"  # noqa
        return self.get_aperture_id(name)

    def get_lightcurve_type_id(self, name):
        try:
            id_ = self.lightcurve_type_cache[name]
        except KeyError:
            id_ = self._resolve_id_from_name(models.LightcurveType, name)
            self.lightcurve_type_cache[name] = id_
        return id_

    def get_best_lightcurve_type_id(self, h5_file):
        name = em2.get_best_detrending_type(h5_file)
        return self.get_lightcurve_type_id(name)

    def process_job(self, em2_h5_job):
        observed_mask = self.get_observed(em2_h5_job.tic_id)

        read_t0 = time()
        with h5py.File(em2_h5_job.file_path, "r") as h5:
            self.telemetry["h5_read"] += time() - read_t0

            cadences = em2.get_cadences(h5)
            bjd = em2.get_barycentric_julian_dates(h5)

            with self.record_elapsed("quality-flag-assignment"):
                quality_flags = self.corrector.get_quality_flags(
                    em2_h5_job.camera, em2_h5_job.ccd, cadences
                )
            with self.record_elapsed("best-lightcurve-construction"):
                best_aperture_id = self.get_best_aperture_id(h5)
                best_type_id = self.get_best_lightcurve_type_id(h5)
                best_lightcurve_definition = {
                    "orbit_id": self.orbit_map[em2_h5_job.orbit_number],
                    "aperture_id": best_aperture_id,
                    "lightcurve_type_id": best_type_id,
                    "tic_id": em2_h5_job.tic_id,
                }
            self.buffers[models.BestOrbitLightcurve.__tablename__].append(
                best_lightcurve_definition
            )
            with self.record_elapsed("lightcurve-construction"):
                data_iter = em2.iterate_for_raw_data(h5)
                for ap_name, type_name, raw_data in data_iter:
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
                    lightcurve = ArrayOrbitLightcurve(
                        tic_id=em2_h5_job.tic_id,
                        camera=em2_h5_job.camera,
                        ccd=em2_h5_job.ccd,
                        orbit_id=self.orbit_map[em2_h5_job.orbit_number],
                        aperture_id=aperture_id,
                        lightcurve_type_id=lightcurve_type_id,
                        cadences=raw_data["cadence"].tolist(),
                        barycentric_julian_dates=raw_data[
                            "barycentric_julian_date"
                        ].tolist(),
                        data=raw_data["data"].tolist(),
                        errors=raw_data["error"].tolist(),
                        x_centroids=raw_data["x_centroid"].tolist(),
                        y_centroids=raw_data["y_centroid"].tolist(),
                        quality_flags=raw_data["quality_flag"].tolist(),
                    )
                    buffer = self.buffers[
                        models.ArrayOrbitLightcurve.__tablename__
                    ]
                    buffer.append(lightcurve)
        self.job_queue.task_done()

    def flush_array_orbit_lightcurves(self, db: DB):
        lightcurves = self.buffers[models.ArrayOrbitLightcurve.__tablename__]
        self.log(f"Flushing {len(lightcurves)} lightcurves")
        start = datetime.now()
        db.bulk_save_objects(lightcurves)
        end = datetime.now()

        metric = models.QLPOperation(
            process_id=self.process.id,
            time_start=start,
            time_end=end,
            job_size=sum(len(lc) for lc in lightcurves),
            unit="lightpoints",
        )
        return metric

    def flush_best_orbit_lightcurves(self, db):
        best_lcs = self.buffers[models.BestOrbitLightcurve.__tablename__]
        self.log(
            "Updating best orbit lightcurve table with "
            f"{len(best_lcs)} entries"
        )
        start = datetime.now()
        q = (
            psql_insert(models.BestOrbitLightcurve)
            .values(best_lcs)
            .on_conflict_do_nothing()
        )
        db.execute(q)
        end = datetime.now()

        metric = models.QLPOperation(
            process_id=self.process.id,
            time_start=start,
            time_end=end,
            job_size=len(best_lcs),
            unit="best_lightcurves",
        )
        self.log_telemetry()
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
        db.flush()
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
        n_lightpoints = sum(
            len(lc.cadences)
            for lc in self.buffers[models.ArrayOrbitLightcurve.__tablename__]
        )
        return n_lightpoints >= self.runtime_parameters["n_lightpoints"]

    @property
    def should_refresh_parameters(self):
        return self.n_samples >= 3


class EM2ArrayParamSearchIngestor(BaseEM2ArrayIngestor):
    step_size = 1000
    max_steps = 10

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

        return {"n_lightpoints": self.step_size * step}


def _initialize_workers(WorkerClass, config, n_processes, **kwargs):
    workers = []
    logger.debug(f"Initializing {n_processes} workers")
    for n in range(n_processes):
        worker = WorkerClass(config, f"worker-{n:02}", **kwargs)  # noqa
        worker.start()
        workers.append(worker)
    logger.debug(f"{n_processes} workers initialized and started")
    return workers


def ingest_jobs(cli_context, jobs, cache_path, poll_rate=1):
    manager = mp.Manager()
    job_queue = manager.Queue()

    if len(jobs) == 0:
        logger.info("No jobs to ingest. Returning")
        return

    with tqdm(total=len(jobs), unit=" jobs") as bar:
        if "logfile" not in cli_context:
            # If logging to standard out, we need to ensure loguru
            # does not step over tqdm output.
            logger.remove()
            logger.add(
                lambda msg: tqdm.write(msg, end=""),
                colorize=True,
                level=cli_context["log_level"].upper(),
                enqueue=True,
            )

        workers: list[EM2ArrayParamSearchIngestor] = _initialize_workers(
            EM2ArrayParamSearchIngestor,
            cli_context["dbconf"],
            cli_context["n_processes"],
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
            for worker in workers:
                worker.join()
            sleep(1 / poll_rate)

    job_queue.join()
