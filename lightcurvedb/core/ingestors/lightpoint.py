import re
from datetime import datetime
from functools import lru_cache
from multiprocessing import Manager
from time import sleep, time

import numpy as np
from click import echo
from loguru import logger
from pgcopy import CopyManager
from psycopg2.errors import InFailedSqlTransaction, UniqueViolation
from sqlalchemy import func, text
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

LC_ERROR_TYPES = {"RawMagnitude"}


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
            observations,
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


def acquire_partition(db, oid):
    indices_q = text(
        f"""
        SELECT pi.schemaname, pi.tablename, pi.indexname, pi.indexdef
        FROM pg_indexes pi
        JOIN pg_class pc ON pc.relname = pi.tablename
        WHERE pc.oid = {oid}
        """
    )
    indices = db.execute(indices_q).fetchall()

    work = []

    for schema, tablename, indexname, indexdf in indices:
        drop_q = text(f"DROP INDEX {schema}.{indexname}")
        work.append(drop_q)

    return work


def release_partition(db, oid):
    track = db.query(RangedPartitionTrack).filter_by(oid=oid).one()

    work = []

    gin_index_name = f"ix_lightpoints_{track.min_range}_{track.max_range}_gin"
    brin_index_name = (
        f"ix_lightpoints_{track.min_range}_{track.max_range}_cadence"
    )

    work.append(
        text(
            f"""
            CREATE INDEX {gin_index_name}
            ON {track.pgclass.namespace.name}.{track.pgclass.name}
            USING gin (lightcurve_id)
            WITH (fastupdate = off)
            """
        )
    )
    work.append(
        text(
            f"""
            CREATE INDEX {brin_index_name}
            ON {track.pgclass.namespace.name}.{track.pgclass.name}
            USING brin (cadence)
            WITH (pages_per_range = 1)
            """
        )
    )
    return work


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
    process_id = None
    current_sample = 0
    n_samples = 0
    seen_cache = set()
    seen_oids = set()
    db = None
    runtime_parameters = {}
    target_table = None
    buffer_order = ["lightpoints", "observations"]

    def __init__(self, config, name, job_queue, stage_id):
        super().__init__(config, name, job_queue)
        self.stage_id = stage_id
        self.log("Initialized")

    def _load_contexts(self):
        self.db = db_from_config(self.db_config)
        with self.db as db, IngestionCache() as cache:
            self.normalizer = TimeCorrector(db, cache)
            self.log("Instantiated bjd normalizer")
            self.quality_flag_map = cache.quality_flag_map
            self.log("Instantiated quality-flag cadence map")
            self.mid_tjd_map = db.get_mid_tjd_mapping()
            self.log("Instantiated mid-tjd cadence map")
            self.orbit_map = dict(db.query(Orbit.orbit_number, Orbit.id))
            self.log("Instantiated Orbit ID map")
            self.set_new_parameters()
            self.log("Determined initial parameters")

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
        aligned_mag = data - mag_offset

        lightcurve["quality_flag"] = quality_flags
        lightcurve["barycentric_julian_date"] = bjd
        lightcurve["data"] = aligned_mag
        return lightcurve

    def process_single_merge_job(self, smj):
        orbit_number = smj.orbit_number
        lightcurve_id = smj.lightcurve_id

        if (lightcurve_id, orbit_number) in self.seen_cache:
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
        except OSError:
            self.log(f"Unable to open {smj.file_path}", level="error")
        except ValueError:
            self.log(f"Unable to process {smj.file_path}", level="error")
        finally:
            self.seen_cache.add((lightcurve_id, orbit_number))

    def process_job(self, job):
        with self.db as db:
            oid = job.partition_oid
            acquire_req = []
            release_req = []
            with scoped_block(db, oid, acquire_req, release_req):
                pgclass = db.query(PGClass).get(oid)
                self.target_table = f"{pgclass.namespace.name}.{pgclass.name}"
                single_merge_jobs = sorted(
                    filter(
                        lambda job: (job.lightcurve_id, job.orbit_number)
                        not in self.seen_cache,
                        job.single_merge_jobs,
                    ),
                    key=lambda job: (job.lightcurve_id, job.orbit_number),
                )

                for single_merge_job in single_merge_jobs:
                    self.process_single_merge_job(single_merge_job)

        n_jobs = len(job.single_merge_jobs)
        self.log(
            f"Finished processing {n_jobs} jobs to {self.target_table}",
            level="success"
        )
        self.job_queue.task_done()

    def flush(self, db):
        ingested = False
        while not ingested:
            try:
                super().flush(db)
                ingested = True
            except (InFailedSqlTransaction, UniqueViolation) as e:
                db.rollback()
                self.log(
                    "Needing to reduce ingestion due to {0}".format(e),
                    level="error",
                )
                match = re.search(
                    r"\((?P<orbit_id>\d+),\s*(?P<lightcurve_id>\d+)\)", str(e)
                )
                orbit_id, lightcurve_id = int(match["orbit_id"]), int(
                    match["lightcurve_id"]
                )
                q = (
                    db.query(func.min(Frame.cadence), func.max(Frame.cadence))
                    .filter(
                        Frame.orbit_id == orbit_id,
                        Frame.frame_type_id == "Raw FFI",
                    )
                    .group_by(Frame.orbit_id)
                )
                min_cadence, max_cadence = q.one()
                self.buffers["observations"] = list(
                    filter(
                        lambda obs: not (
                            obs[0] == lightcurve_id and obs[1] == orbit_id
                        ),
                        self.buffers["observations"],
                    )
                )
                for lp_buffer in self.buffers["lightpoints"]:
                    id_mask = lp_buffer["lightcurve_id"] == lightcurve_id
                    cadence_mask = (min_cadence <= lp_buffer["cadence"]) & (
                        lp_buffer["cadence"] <= max_cadence
                    )
                    lp_buffer = lp_buffer[~(id_mask & cadence_mask)]
                self.log(
                    "Reduced ingestion work due to observation collision",
                    level="warning",
                )

        self.n_samples += 1

        if self.should_refresh_parameters:
            self.set_new_parameters()

    def flush_lightpoints(self, db):
        lps = self.buffers.get("lightpoints", [])
        if len(lps) < 1:
            return

        conn = db.session.connection().connection
        lp_size = sum(len(chunk) for chunk in lps)
        self.log(f"Flushing {lp_size} lightpoints to remote", level="debug")
        mgr = CopyManager(conn, self.target_table, Lightpoint.get_columns())
        start = datetime.now()

        while len(lps) > 0:
            chunk = lps.pop()
            mgr.threading_copy(chunk)

        end = datetime.now()

        assert db.query(QLPProcess).filter(QLPProcess.id == self.process_id).count() > 0
        metric = QLPOperation(
            process_id=self.process_id,
            time_start=start,
            time_end=end,
            job_size=lp_size,
            unit="lightpoint",
        )
        db.add(metric)

    def flush_observations(self, db):
        obs = self.buffers.get("observations", [])
        if len(obs) < 1:
            return
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

    def set_new_parameters(self):
        with self.db as db:
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
            if self.process_id is not None:
                q = (
                    db
                    .query(QLPProcess)
                    .filter(
                        QLPProcess.id == self.process_id
                    )
                )
                q.update(
                    {"state": "completed"},
                    synchronize_session=False
                )

            db.session.refresh(process)
            db.commit()
            self.log(f"Added {process}")
            self.process_id = process.id
            self.runtime_parameters = process.runtime_parameters

        self.n_samples = 0

    @property
    def should_flush(self):
        return (
            len(self.buffers["lightpoints"])
            >= self.runtime_parameters["lp_buffer_threshold"]
        )

    @property
    def should_refresh_parameters(self):
        return self.n_samples >= 3


class SamplingLightpointIngestor(BaseLightpointIngestor):
    max_lp_buffersize = 10e5
    min_lp_buffersize = 1

    def determine_process_parameters(self):
        lp_buffersize = np.random.randint(
            self.min_lp_buffersize, high=self.max_lp_buffersize + 1
        )
        return {
            "lp_buffer_threshold": lp_buffersize,
        }


class ExponentialSamplingLightpointIngestor(BaseLightpointIngestor):
    max_exponent = 24
    min_exponent = 3

    def determine_process_parameters(self):
        exp = np.random.randint(self.min_exponent, high=self.max_exponent)
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
    n_todo = len(jobs)
    with tqdm(total=len(jobs), unit=" jobs") as bar:
        while len(jobs) > 0:
            job = jobs.pop()
            job_queue.put(job)
            total_single_jobs += len(job.single_merge_jobs)
            bar.update(1)

    with db:
        echo("Grabbing introspective processing tracker")
        stage = db.get_qlp_stage("lightpoint-ingestion")

    echo("Initializing workers, beginning processing...")
    with tqdm(total=len(jobs)) as bar:
        logger.remove()
        logger.add(
            lambda msg: tqdm.write(msg, end=""),
            colorize=False,
            level=log_level.upper(),
            enqueue=True,
        )
        for n in range(n_processes):
            p = ExponentialSamplingLightpointIngestor(
                db._config, f"worker-{n}", job_queue, stage.id
            )
            p.start()

        # Wait until all jobs have been pulled off queue
        while not job_queue.empty():
            queue_size = job_queue.qsize()
            n_done = n_todo - queue_size
            bar.update(n_done)
            n_todo = queue_size
            sleep(5)

        logger.debug("Job queue empty, waiting for worker exits")
        for worker in workers:
            worker.join()

        logger.debug("Joining work queues")
        job_queue.join()
