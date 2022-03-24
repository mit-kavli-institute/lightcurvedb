import re
import warnings
from random import sample
from collections import defaultdict
from datetime import datetime
from functools import lru_cache
from multiprocessing import Manager
from time import sleep, time

import numpy as np
from click import echo
from loguru import logger
from pgcopy import CopyManager
from psycopg2.errors import InFailedSqlTransaction, UniqueViolation
from queue import Empty
from sqlalchemy import func, text, Integer
from sqlalchemy.exc import InternalError
from sqlalchemy.sql.expression import cast
from tqdm import tqdm

from lightcurvedb import db_from_config
from lightcurvedb.core.ingestors.cache import IngestionCache
from lightcurvedb.core.ingestors.consumer import BufferedDatabaseIngestor
from lightcurvedb.core.ingestors.lightcurve_ingestors import (
    get_components,
    h5_to_numpy,
)
from lightcurvedb.core.psql_tables import PGClass
from lightcurvedb.core.tic8 import TIC8_DB
from lightcurvedb.io.pipeline.scope import scoped_block
from lightcurvedb.legacy.timecorrect import TimeCorrector
from lightcurvedb.models import Frame, Lightpoint, Observation, Orbit
from lightcurvedb.models.metrics import QLPOperation, QLPProcess
from lightcurvedb.models.table_track import RangedPartitionTrack


def _load_tic_parameters(distinct_run_setups):
    raise NotImplementedError


@lru_cache(maxsize=16)
def query_tic(tic, *fields):
    logger.warning(
        f"Could not find TIC {tic} in cache. "
        f"Querying remote db for fields {fields}"
    )
    with TIC8_DB() as tic8:
        ticentries = tic8.ticentries
        columns = tuple(getattr(ticentries.c, field) for field in fields)

        q = tic8.query(*columns).filter(ticentries.c.id == tic)
        results = q.one()
    return dict(zip(fields, results))


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

    def __init__(self, config, name, job_queue, stage_id):
        super().__init__(config, name, job_queue)
        self.stage_id = stage_id
        self.log("Initialized")

    def _load_contexts(self):
        with self.db as db, IngestionCache() as cache:
            self.normalizer = TimeCorrector(db, cache)
            self.log("Instantiated bjd normalizer")
            self.quality_flag_map = cache.quality_flag_map
            self.log("Instantiated quality-flag cadence map")
            self.mid_tjd_map = db.get_mid_tjd_mapping()
            self.log("Instantiated mid-tjd cadence map")
            self.orbit_map = dict(db.query(Orbit.orbit_number, Orbit.id))
            self.log("Instantiated Orbit ID map")
            self.rng = np.random.default_rng()
            self.set_new_parameters(db)
            self.log("Determined initial parameters")

    def _postflush(self, db):
        self.n_samples += 1

        if self.should_refresh_parameters:
            self.set_new_parameters(db)

    def get_quality_flags(self, camera, ccd, cadences):
        qflag_df = self.quality_flag_map[(camera, ccd)].loc[cadences]
        return qflag_df.quality_flag.to_numpy()

    def get_mid_tjd(self, camera, cadences):
        return self.mid_tjd_map[camera].loc[cadences]["mid_tjd"].to_numpy()

    def get_mag_alignment_offset(self, data, quality_flags, tmag):
        mask = quality_flags == 0
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            return np.nanmedian(data[mask]) - tmag

    def process_h5(self, id_, aperture, lightcurve_type, h5path):
        self.log(f"Processing {h5path}", level="trace")
        lightcurve = h5_to_numpy(id_, aperture, lightcurve_type, h5path)
        context = get_components(h5path)
        tic_id, camera, ccd = (
            int(context["tic_id"]),
            int(context["camera"]),
            int(context["ccd"]),
        )
        row = self.normalizer.get_tic_params(tic_id)
        tmag = row["tmag"]
        ra, dec = row["ra"], row["dec"]

        if np.isnan(ra) or np.isnan(dec):
            self.log(
                f"Star coordinates undefined for {tic_id} ({ra}, {dec})",
                level="error",
            )
            raise ValueError

        cadences = lightcurve["cadence"]
        data = lightcurve["data"]

        quality_flags = self.get_quality_flags(camera, ccd, cadences)
        mid_tjd = self.get_mid_tjd(camera, cadences)
        bjd = self.normalizer.correct(tic_id, mid_tjd)
        mag_offset = self.get_mag_alignment_offset(data, quality_flags, tmag)
        if np.isnan(mag_offset):
            self.log(
                f"{tic_id} {aperture} {lightcurve_type} orbit " 
                f"{context['orbit_number']} returned NaN for "
                "alignment offset",
                level="warning"
            )
        aligned_mag = data - mag_offset

        lightcurve["quality_flag"] = quality_flags
        lightcurve["barycentric_julian_date"] = bjd
        lightcurve["data"] = aligned_mag
        return lightcurve

    def process_job(self, smj):
        orbit_number = smj.orbit_number
        lightcurve_id = smj.lightcurve_id

        if (lightcurve_id, orbit_number) in self.seen_cache:
            self.log(f"Ignoring duplicate job")
            return None

        try:
            lightpoint_array = self.process_h5(
                smj.lightcurve_id,
                smj.aperture,
                smj.lightcurve_type,
                smj.file_path,
            )
            observation = (
                smj.lightcurve_id,
                self.orbit_map[smj.orbit_number],
                smj.camera,
                smj.ccd,
            )
            self.buffers["lightpoints"].append(lightpoint_array)
            self.buffers["observations"].append(observation)
        except OSError as e:
            self.log(f"Unable to open {smj.file_path}: {e}", level="exception")
        except ValueError:
            self.log(f"Unable to process {smj.file_path}", level="error")
        finally:
            self.seen_cache.add((lightcurve_id, orbit_number))
            self.job_queue.task_done()

    def flush_lightpoints(self, db):
        lps = self.buffers.get("lightpoints")

        conn = db.session.connection().connection
        lp_size = sum(len(chunk) for chunk in lps)
        self.log(
            f"Flushing {lp_size} lightpoints over {len(lps)} jobs to remote",
            level="debug"
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
            q = (
                db
                .query(QLPProcess)
                .filter(
                    QLPProcess.id == self.process.id
                )
            )
            q.update(
                {"state": "completed"},
                synchronize_session=False
            )

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
                lightcurve_id=lc_id,
                orbit_id=orbit_id,
                camera=camera,
                ccd=ccd
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
                except:
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
    min_exponent = 9

    def determine_process_parameters(self):
        # Force to python int, np.int64 is not JSON compatible
        job_size_log2 = cast(func.log(2, QLPOperation.job_size), Integer)

        q = (
            self
            .db
            .query(job_size_log2, func.Count(QLPOperation.id))
            .join(QLPOperation.process)
            .filter(QLPProcess.current_version())
            .group_by(job_size_log2)
        )

        current_samples = dict(q)
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
    db, jobs, n_processes, commit, log_level="info", worker_class=None
):
    """
    Process and ingest SingleMergeJob objects.
    """
    workers = []
    manager = Manager()
    job_queue = manager.Queue()
    total_single_jobs = 0

    echo("Enqueing multiprocessing work")
    total_single_merge_jobs = len(jobs)
    distinct_run_setups = set()

    for job in tqdm(jobs, unit=" jobs"):
        job_queue.put(job)
        distinct_run_setups.add((job.orbit_number, job.camera, job.ccd))

    echo("Obtaining common contexts")
    for orbit_number, camera, ccd in 

    with db:
        echo("Grabbing introspective processing tracker")
        stage = db.get_qlp_stage("lightpoint-ingestion")

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
                db._config, f"worker-{n}", job_queue, stage.id
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
        for worker in workers:
            worker.join()
            logger.debug(str(worker))
        logger.debug("Job queue empty, cleaning")
