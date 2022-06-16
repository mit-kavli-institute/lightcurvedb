from collections import defaultdict
from datetime import datetime
from multiprocessing import Manager
from queue import Empty
from random import sample
from time import sleep

import numpy as np
from click import echo
from h5py import File as H5File
from loguru import logger
from pgcopy import CopyManager
from sqlalchemy import Integer, func
from sqlalchemy.exc import DataError
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql.expression import cast
from tqdm import tqdm

from lightcurvedb import db_from_config
from lightcurvedb.core.ingestors.consumer import BufferedDatabaseIngestor
from lightcurvedb.core.ingestors.correction import LightcurveCorrector
from lightcurvedb.core.ingestors.lightcurves import (
    bjd_from_h5_fd,
    cadences_from_h5_fd,
    get_components,
    h5_fd_to_numpy,
)
from lightcurvedb.models import Lightpoint, Observation, Orbit
from lightcurvedb.models.metrics import QLPOperation, QLPProcess, QLPStage


class BaseLightpointIngestor(BufferedDatabaseIngestor):
    normalizer = None
    quality_flag_map = None
    mid_tjd_map = None
    orbit_map = None
    stage_id = None
    process = None
    current_sample = 0
    n_samples = 0
    seen_cache = set()
    db = None
    runtime_parameters = {}
    rng = None
    target_table = "lightpoints"
    buffer_order = ["lightpoints", "observations"]

    def __init__(self, config, name, job_queue, stage_id, cache_path):
        super().__init__(config, name, job_queue)
        self.cache_path = cache_path
        self.stage_id = stage_id
        self.log("Initialized")

    def _load_contexts(self):
        try:
            with self.db as db:
                self.corrector = LightcurveCorrector(self.cache_path)
                self.orbit_map = dict(db.query(Orbit.orbit_number, Orbit.id))
                self.log("Instantiated Orbit ID map")
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

    def read_lightcurve(
        self, id_, aperture, lightcurve_type, quality_flags, context, h5
    ):
        lightcurve = h5_fd_to_numpy(id_, aperture, lightcurve_type, h5)
        tic_id = int(context["tic_id"])

        magnitudes = lightcurve["data"]

        # tmag-alignment
        mag_offset = self.corrector.get_magnitude_alignment_offset(
            tic_id, magnitudes, quality_flags
        )
        if np.isnan(mag_offset):
            self.log(
                f"{tic_id} {aperture} {lightcurve_type} orbit "
                f"{context['orbit_number']} returned NaN for "
                "alignment offset",
                level="warning",
            )
        aligned_mag = magnitudes - mag_offset

        lightcurve["data"] = aligned_mag
        return lightcurve

    def process_job(self, h5_job):
        self.log(f"Processing {h5_job.file_path}", level="trace")
        context = get_components(h5_job.file_path)
        with H5File(h5_job.file_path, "r") as h5:
            cadences = cadences_from_h5_fd(h5)
            bjd = bjd_from_h5_fd(h5)
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

            for smj in h5_job.single_merge_jobs:
                orbit_number = smj.orbit_number
                lightcurve_id = smj.lightcurve_id
                if (lightcurve_id, orbit_number) in self.seen_cache:
                    self.log("Ignoring duplicate job")
                    return None

                try:
                    lightpoint_array = self.read_lightcurve(
                        smj.lightcurve_id,
                        smj.aperture,
                        smj.lightcurve_type,
                        quality_flags,
                        context,
                        h5,
                    )
                    lightpoint_array["barycentric_julian_date"] = bjd[0]
                    lightpoint_array["quality_flag"] = quality_flags
                    observation = (
                        smj.lightcurve_id,
                        self.orbit_map[smj.orbit_number],
                        smj.camera,
                        smj.ccd,
                    )
                    self.buffers["lightpoints"].append(lightpoint_array)
                    self.buffers["observations"].append(observation)
                except OSError as e:
                    self.log(
                        f"Unable to open {smj.file_path}: {e}",
                        level="exception",
                    )
                except ValueError as e:
                    self.log(
                        f"Unable to process {smj.file_path}: {e}",
                        level="exception",
                    )
                finally:
                    self.seen_cache.add((lightcurve_id, orbit_number))

        self.job_queue.task_done()

    def flush_lightpoints(self, db):
        lps = self.buffers.get("lightpoints")

        conn = db.session.connection().connection
        lp_size = sum(len(chunk) for chunk in lps)
        self.log(
            f"Flushing {lp_size} lightpoints over {len(lps)} jobs to remote",
            level="debug",
        )
        mgr = CopyManager(conn, self.target_table, Lightpoint.get_columns())
        start = datetime.now()

        for chunk in lps:
            mgr.threading_copy(chunk)

        end = datetime.now()

        metric = QLPOperation(
            process_id=self.process.id,
            time_start=start,
            time_end=end,
            job_size=lp_size,
            unit="lightpoint",
        )
        return metric

    def flush_observations(self, db):
        obs = self.buffers.get("observations")
        self.log(f"Flushing {len(obs)} observations to remote", level="trace")
        conn = db.session.connection().connection
        mgr = CopyManager(
            conn,
            Observation.__tablename__,
            ["lightcurve_id", "orbit_id", "camera", "ccd"],
        )
        mgr.threading_copy(obs)

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
            sum(len(array) for array in self.buffers["lightpoints"])
            >= self.runtime_parameters["lp_buffer_threshold"]
        )

    @property
    def should_refresh_parameters(self):
        return self.n_samples >= 3


class ImmediateLightpointIngestor(BaseLightpointIngestor):
    def _execute_job(self, db, job):
        self.process_job(job)
        metric = self.flush_lightpoints(db)
        for lc_id, orbit_id, camera, ccd in self.buffers.get("observations"):
            obs = Observation(
                lightcurve_id=lc_id, orbit_id=orbit_id, camera=camera, ccd=ccd
            )
            db.add(obs)
        db.add(metric)
        db.commit()

        # Clear buffers
        for buffer_key in self.buffer_order:
            self.buffers[buffer_key] = []

    def run(self):
        self.log("Entering main runtime")
        self.db = db_from_config(self.db_config)
        self._load_contexts()
        with self.db as db:
            while not self.job_queue.empty():
                try:
                    job = self.job_queue.get(timeout=10)
                    self._execute_job(db, job)
                except Empty:
                    self.log("Timed out", level="error")
                    break
                except KeyboardInterrupt:
                    self.log("Received keyboard interrupt")
                    break
                except Exception:
                    self.log("Breaking", level="exception")
                    break
        self.log("Finished, exiting main runtime")

    def determine_process_parameters(self):
        return {}


class SamplingLightpointIngestor(BaseLightpointIngestor):
    max_lp_buffersize = 10e5
    min_lp_buffersize = 1
    bucket_size = 100

    def determine_process_parameters(self):
        lp_buffersize = np.random.randint(
            self.min_lp_buffersize, high=self.max_lp_buffersize + 1
        )
        return {
            "lp_buffer_threshold": lp_buffersize,
        }


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

        return {"lp_buffer_threshold": 2 ** exp}


def ingest_merge_jobs(
    db, jobs, n_processes, cache_path, log_level="info", worker_class=None
):
    """
    Process and ingest SingleMergeJob objects.
    """
    workers = []
    manager = Manager()
    job_queue = manager.Queue()

    echo("Enqueing multiprocessing work")
    distinct_run_setups = set()

    for job in tqdm(jobs, unit=" jobs"):
        job_queue.put(job)
        for smj in job.single_merge_jobs:
            distinct_run_setups.add((smj.orbit_number, smj.camera, smj.ccd))

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
    with tqdm(total=len(jobs)) as bar:
        logger.remove()
        logger.add(
            lambda msg: tqdm.write(msg, end=""),
            colorize=True,
            level=log_level.upper(),
            enqueue=True,
        )
        for n in range(n_processes):
            p = ExponentialSamplingLightpointIngestor(
                db._config, f"worker-{n}", job_queue, stage.id, cache_path
            )
            p.start()
            workers.append(p)

        # Wait until all jobs have been pulled off queue
        prev = job_queue.qsize()
        while not job_queue.empty():
            cur = job_queue.qsize()
            diff = prev - cur
            bar.update(diff)
            prev = cur
            sleep(1)

        bar.update(prev)
        job_queue.join()

        # Work queue is done, only allow workers ~10 minutes to wrap it up.
        for worker in workers:
            worker.join(10)

            if worker.exitcode is None:
                logger.error(f"Terminating worker {worker}, took too long")
                worker.terminate()
            else:
                logger.debug(
                    f"Worker {worker} finished with "
                    f"exit code: {worker.exitcode}"
                )
