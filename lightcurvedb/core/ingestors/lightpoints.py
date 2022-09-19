import pathlib
from collections import defaultdict
from datetime import datetime
from multiprocessing import Manager
from os import getpid
from random import sample
from tempfile import TemporaryDirectory
from time import sleep

import numpy as np
from click import echo
from h5py import File as H5File
from loguru import logger
from pgcopy import CopyManager
from psycopg2.errors import InFailedSqlTransaction
from sqlalchemy import Integer, func
from sqlalchemy.exc import DataError
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql.expression import cast
from tqdm import tqdm

from lightcurvedb.core.ingestors.consumer import BufferedDatabaseIngestor
from lightcurvedb.core.ingestors.correction import LightcurveCorrector
from lightcurvedb.core.ingestors.lightcurves import (
    best_detrending_from_h5_fd,
    cadences_from_h5_fd,
    h5_fd_to_numpy,
)
from lightcurvedb.models import (
    Aperture,
    BestOrbitLightcurve,
    LightcurveType,
    Lightpoint,
    Orbit,
    OrbitLightcurve,
)
from lightcurvedb.models.metrics import QLPOperation, QLPProcess, QLPStage
from lightcurvedb.util.contexts import extract_pdo_path_context


def _yield_lightpoints(files, lightcurves):
    for f, orbit_lightcurve in zip(files, lightcurves):
        id_ = orbit_lightcurve.id
        array = np.load(f)
        for _, cadence, bjd, data, err, x, y, flag in array:
            yield (id_, cadence, bjd, data, err, x, y, flag)


class BaseLightpointIngestor(BufferedDatabaseIngestor):
    target_table = Lightpoint.__tablename__
    buffer_order = [
        "orbit_lightcurves",
        "best_orbit_lightcurves",
        "lightpoints",
    ]

    def __init__(
        self, config, name, job_queue, stage_slug, cache_path, lp_cache
    ):
        super().__init__(config, name, job_queue)
        self.cache_path = cache_path
        self.stage_slug = stage_slug
        self.log("Initialized")
        self.apertures = {}
        self.lightcurve_types = {}
        self.bestap_cache = {}
        self.best_detrend_cache = {}
        self.runtime_parameters = {}
        self.tmp_lc_id_map = {}
        self.orbit_map = {}
        self.current_sample = 0
        self.n_samples = 0
        self.rng = None
        self.process = None
        self.lp_cache = pathlib.Path(lp_cache)
        self.n_lightpoints = 0

    def _load_contexts(self):
        try:
            with self.db as db:
                self.corrector = LightcurveCorrector(self.cache_path)
                self.orbit_map = dict(db.query(Orbit.orbit_number, Orbit.id))
                self.log("Instantiated Orbit ID map")

                stage = (
                    self.db.query(QLPStage)
                    .filter_by(slug=str(self.stage_slug))
                    .one()
                )
                self.stage_id = stage.id
                self.log(f"Will use stage metric {stage}")
                self.rng = np.random.default_rng()
                self.set_new_parameters(db)
                self.log("Determined initial parameters")
        except Exception as e:
            self.log(
                f"Unable to load contexts. Encountered {e}", level="exception"
            )
            raise

    def _postflush(self, db):
        self.n_samples += 1

        if self.should_refresh_parameters:
            self.set_new_parameters(db)

    def get_aperture_id(self, name):
        try:
            id_ = self.apertures[name]
        except KeyError:
            with self.db as db:
                q = db.query(Aperture.id).filter_by(name=name)
                id_ = q.one()[0]
                self.apertures[name] = id_
        return id_

    def get_lightcurve_type_id(self, name):
        try:
            id_ = self.lightcurve_types[name]
        except KeyError:
            with self.db as db:
                q = db.query(LightcurveType.id).filter_by(name=name)
                id_ = q.one()[0]
                self.lightcurve_types[name] = id_
        return id_

    def get_best_aperture_id(self, tic_id):
        tmag = self.corrector.resolve_tic_parameters(tic_id, "tmag")[0]
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
        name = best_detrending_from_h5_fd(h5_file)
        try:
            id_ = self.best_detrend_cache[name]
        except KeyError:
            self.log(f"Best Detrending cache miss for {name}, resolving")
            with self.db as db:
                id_ = db.resolve_best_lightcurve_type_id(name)
            self.best_detrend_cache[name] = id_
        return id_

    def read_lightcurve(
        self, id_, aperture, lightcurve_type, quality_flags, context, h5
    ):
        lightcurve = h5_fd_to_numpy(id_, aperture, lightcurve_type, h5)
        return lightcurve

    def process_job(self, h5_job):
        self.log(f"Processing {h5_job.file_path}", level="trace")
        context = extract_pdo_path_context(h5_job.file_path)
        with H5File(h5_job.file_path, "r") as h5:
            cadences = cadences_from_h5_fd(h5)
            tic_id, camera, ccd = (
                int(context["tic_id"]),
                int(context["camera"]),
                int(context["ccd"]),
            )
            mid_tjd = self.corrector.get_mid_tjd(camera, cadences)
            bjd = self.corrector.correct_for_earth_time(tic_id, mid_tjd)
            quality_flags = self.corrector.get_quality_flags(
                camera, ccd, cadences
            )
            bestap_id = self.get_best_aperture_id(tic_id)
            best_detrend_id = self.get_best_detrend_id(h5)

            for orbit_job in h5_job.orbit_lightcurve_jobs:
                aperture_id = self.get_aperture_id(orbit_job.aperture)
                lightcurve_type_id = self.get_lightcurve_type_id(
                    orbit_job.lightcurve_type
                )
                orbit_number = orbit_job.orbit_number
                orbit_id = self.orbit_map[orbit_number]

                best_lightcurve_definition = {
                    "orbit_id": orbit_id,
                    "aperture_id": bestap_id,
                    "lightcurve_id": best_detrend_id,
                    "tic_id": tic_id,
                }

                try:
                    pos = len(self.buffers["orbit_lightcurves"])
                    lightpoint_array = self.read_lightcurve(
                        pos,
                        orbit_job.aperture,
                        orbit_job.lightcurve_type,
                        quality_flags,
                        context,
                        h5,
                    )
                    lightcurve = OrbitLightcurve(
                        tic_id=tic_id,
                        camera=camera,
                        ccd=ccd,
                        aperture_id=aperture_id,
                        lightcurve_type_id=lightcurve_type_id,
                        orbit_id=orbit_id,
                    )
                    if orbit_job.preassigned_id is not None:
                        lightcurve.id = orbit_job.preassigned_id

                    lightpoint_array["barycentric_julian_date"] = bjd
                    lightpoint_array["quality_flag"] = quality_flags

                    file_path = self.lp_cache / f"{pos}_{getpid()}_lp_blob.npy"
                    np.save(file_path, lightpoint_array)
                    self.n_lightpoints += len(lightpoint_array)

                    self.buffers["lightpoints"].append(file_path)
                    self.buffers["orbit_lightcurves"].append(lightcurve)
                    self.buffers["best_orbit_lightcurves"].append(
                        best_lightcurve_definition
                    )
                except OSError as e:
                    self.log(
                        f"Unable to open {orbit_job.file_path}: {e}",
                        level="exception",
                    )
                except ValueError as e:
                    self.log(
                        f"Unable to process {orbit_job.file_path}: {e}",
                        level="exception",
                    )

        queue_tries = 5
        while queue_tries > 0:
            try:
                self.job_queue.task_done()
                break
            except ConnectionResetError:
                self.log("Could not mark job as done, waiting...")
                wait_time = 2 ** (5 - queue_tries)
                sleep(wait_time)
                queue_tries -= 1

        if queue_tries == 0:
            raise RuntimeError(
                f"{self.name} could not properly communicate with job queue"
            )

    def flush_lightpoints(self, db):
        """
        Flush lightpoints from buffers to remote. At this point lightpoints
        have temporary lightcurve ids assigned and must be updated with the
        ids assigned from remote.
        """
        files = self.buffers["lightpoints"]
        lcs = self.buffers["orbit_lightcurves"]

        conn = db.session.connection().connection
        start = datetime.now()
        lp_size = self.n_lightpoints

        self.log(
            f"Flushing {lp_size:,} lightpoints over "
            f"{len(files):,} jobs to remote",
            level="debug",
        )

        try:
            mgr = CopyManager(
                conn, self.target_table, Lightpoint.get_columns()
            )
            mgr.threading_copy(_yield_lightpoints(files, lcs))
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                _healthcheck = cur.fetchall()  # noqa F841
        except InFailedSqlTransaction:
            # threading failed silently, raise:
            raise RuntimeError

        end = datetime.now()

        # Remove files there was a successful push
        for f in files:
            f.unlink()
        self.n_lightpoints = 0

        metric = QLPOperation(
            process_id=self.process.id,
            time_start=start,
            time_end=end,
            job_size=lp_size,
            unit="lightpoint",
        )
        return metric

    def flush_orbit_lightcurves(self, db):
        lightcurves = self.buffers.get("orbit_lightcurves")
        self.log(f"Flushing {len(lightcurves):,} orbit lightcurves to remote")

        start = datetime.now()
        db.session.add_all(lightcurves)
        db.flush()
        end = datetime.now()
        # Ids should now be assigned.

        metric = QLPOperation(
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
            "Updating best orbit lightcurve table with "
            f"{len(best_lcs)} entries"
        )

        start = datetime.now()
        db.session.bulk_insert_mapping(BestOrbitLightcurve, best_lcs)
        end = datetime.now()

        metric = QLPOperation(
            process_id=self.process.id,
            time_start=start,
            time_end=end,
            job_size=len(best_lcs),
            unit="best_lightcurves",
        )
        return metric

    def determine_process_parameters(self):
        raise NotImplementedError

    def set_new_parameters(self, db):
        self.log(f"Setting new parameters. DB State: {db}")
        process = QLPProcess(
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
            q = db.query(QLPProcess).filter(QLPProcess.id == self.process.id)
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


class ExponentialSamplingLightpointIngestor(BaseLightpointIngestor):
    max_exponent = 24
    min_exponent = 11

    def determine_process_parameters(self):
        # Force to python int, np.int64 is not JSON compatible
        job_size_log2 = cast(
            func.log(
                2,
                (
                    QLPProcess.runtime_parameters["lp_buffer_threshold"].cast(
                        Integer
                    )
                ),
            ),
            Integer,
        )

        q = (
            self.db.query(job_size_log2, func.Count(QLPOperation.id))
            .join(QLPOperation.process)
            .filter(QLPProcess.current_version())
            .group_by(job_size_log2)
        )
        try:
            current_samples = dict(q)
        except DataError:
            self.log(
                "Unable to take logarithm, assuming no previous datapoints"
            )
            current_samples = {}
            self.db.rollback()

        samples = defaultdict(list)
        for exp in range(self.min_exponent, self.max_exponent + 1):
            n_samples = current_samples.get(exp, 0)
            samples[n_samples].append(exp)

        lowest_sample_rate = min(samples.keys())
        possible_exp = samples[lowest_sample_rate]

        # naively pick first
        exp = sample(possible_exp, 1)[0]

        return {"lp_buffer_threshold": 2**exp}


class StepSamplingLightpointIngestor(BaseLightpointIngestor):
    step_size = 800
    max_steps = 62500

    def determine_process_parameters(self):
        step_col = cast(QLPOperation.job_size / self.step_size, Integer)

        q = (
            self.db.query(
                step_col.label("bucket"),
                func.count(QLPOperation.id),
            )
            .join(QLPOperation.process)
            .filter(QLPProcess.current_version())
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


def _queue_is_empty(queue):
    wait = 1
    while True:
        try:
            return queue.empty()
        except ConnectionResetError:
            unit = "second" if wait == 1 else "seconds"
            logger.warning(
                "Main thread could not communicate with queue, "
                f"waiting {wait} {unit}."
            )
            sleep(wait)
            wait += 1


def ingest_merge_jobs(
    db,
    jobs,
    n_processes,
    cache_path,
    lp_scratch,
    log_level="info",
    worker_class=None,
):
    """
    Process and ingest SingleMergeJob objects.
    """
    workers = []
    manager = Manager()
    job_queue = manager.Queue()

    echo("Enqueing multiprocessing work")

    for job in tqdm(jobs, unit=" jobs"):
        job_queue.put(job)

    with db:
        echo("Grabbing introspective processing tracker")
        try:
            stage = db.get_qlp_stage("lightpoint-ingestion")
        except NoResultFound:
            db.rollback()
            stage = QLPStage(
                name="Lightpoint Ingestion", slug="lightpoint-ingestion"
            )
            db.add(stage)
            db.commit()
            db.session.refresh(stage)

    echo("Initializing workers, beginning processing...")
    with tqdm(total=len(jobs)) as bar, TemporaryDirectory(
        dir=lp_scratch
    ) as tmpdir:
        logger.remove()
        logger.add(
            lambda msg: tqdm.write(msg, end=""),
            colorize=True,
            level=log_level.upper(),
            enqueue=True,
        )
        for n in range(n_processes):
            p = StepSamplingLightpointIngestor(
                db._config,
                f"worker-{n}",
                job_queue,
                "lightpoint-ingestion",
                cache_path,
                tmpdir,
            )
            p.start()
            workers.append(p)

        # Wait until all jobs have been pulled off queue
        prev = job_queue.qsize()
        while not _queue_is_empty(job_queue):
            cur = job_queue.qsize()
            diff = prev - cur
            bar.update(diff)
            prev = cur
            sleep(1)

        bar.update(prev)
        job_queue.join()

        # Work queue is done, only allow workers ~10 minutes to wrap it up.
        for worker in workers:
            worker.join(60 * 10)

            if worker.exitcode is None:
                logger.error(f"Terminating worker {worker}, took too long")
                worker.terminate()
            else:
                logger.debug(
                    f"Worker {worker} finished with "
                    f"exit code: {worker.exitcode}"
                )
