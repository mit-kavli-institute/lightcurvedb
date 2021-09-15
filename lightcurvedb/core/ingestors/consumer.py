from lightcurvedb import db_from_config
from lightcurvedb.core.engines import psycopg_connection
from lightcurvedb.core.ingestors.lightpoint import LightpointProcessor
from lightcurvedb.models import Lightpoint, Frame, Orbit
from lightcurvedb.models.table_track import RangedPartitionTrack
from lightcurvedb.core.psql_tables import PGClass, PGNamespace
from lightcurvedb.core.ingestors.lightpoint import PartitionConsumer, push_lightpoints, push_observations
from lightcurvedb.io.pipeline.scope import scoped_block
from loguru import logger
from psycopg2.errors import UniqueViolation

from functools import lru_cache
from lightcurvedb import db_from_config
from pgcopy import CopyManager
from sqlalchemy import text
from time import time

import warnings
import numpy as np
import pandas as pd


def acquire_partition(db, oid):
    track = (
        db
        .query(RangedPartitionTrack)
        .filter_by(oid=oid)
        .one()
    )
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
    track = (
        db
        .query(RangedPartitionTrack)
        .filter_by(oid=oid)
        .one()
    )

    work = []

    gin_index_name = f"ix_lightpoints_{track.min_range}_{track.max_range}_gin"
    brin_index_name = f"ix_lightpoints_{track.min_range}_{track.max_range}_cadence"

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


class FullPartitionConsumer(PartitionConsumer):

    orbit_id_map = None
    orbit_number_map = None
    cache = set()
    commit_threshold = 60

    def clean_lp_array(self, lp):
        # Only allow monotonic increasing cadence #
        path = np.argsort(lp, order=["lightcurve_id", "cadence"])
        val_diff = np.insert(
            np.diff(lp["cadence"][path]),
            0,
            1
        )
        # Remove duplicate cadence
        return lp[path[val_diff != 0]]

    def orbit_from_lp(self, lp):
        cadences = lp["cadence"]
        orbit_id = {self.orbit_id_map[c] for c in cadences}
        orbit_number = {self.orbit_number_map[c] for c in cadences}
        if len(orbit_id) != 1 or len(orbit_number) != 1:
            raise RuntimeError(
                "Given lightpoint array contained more than 1 orbit"
            )
        return {
            "orbit_id": orbit_id.pop(),
            "orbit_number": orbit_number.pop(),
        }

    def process_single_merge_job(self, smj):
        unique_key = (
            smj.lightcurve_id,
            smj.orbit_number
        )
        if unique_key in self.cache:
            self.log(
                f"Skipping {unique_key}",
                level="warning"
            )
            return None, {
                "file_load": 0,
                "quality_flag_assignment": 0,
                "bjd_correction": 0,
                "alignment": 0,
                "validation_time": 0,
            }
        try:
            lp, timing = self.process_h5(
                smj.lightcurve_id,
                smj.aperture,
                smj.lightcurve_type,
                smj.file_path
            )
            stamp = time()
            lp = self.clean_lp_array(lp)
            timing["validation_time"] = time() - stamp
        except OSError:
            self.log(
                f"Unable to open {smj.file_path}",
                level="warning"
            )
            timing = {
                "file_load": 0,
                "quality_flag_assignment": 0,
                "bjd_correction": 0,
                "alignment": 0,
                "validation_time": 0,
            }
            lp = None

        self.cache.add(unique_key)

        return lp, timing

    def process_job(self, partition_job):
        lps = []
        observations = []
        timings = []
        validation_time = 0
        total_points = 0
        copy_elapsed = 0

        self.cache = set()

        with db_from_config() as db:
            track = (
                db
                .query(RangedPartitionTrack)
                .filter(
                    RangedPartitionTrack.oid == partition_job.partition_oid
                )
                .one()
            )
            target_table = f"{track.pgclass.namespace.name}.{track.pgclass.name}"
            acquire_actions = acquire_partition(db, partition_job.partition_oid)
            release_actions = release_partition(db, partition_job.partition_oid)

            with scoped_block(db, None, acquire_actions, release_actions):
                conn = db.session.connection().connection
                mgr = CopyManager(conn, target_table, Lightpoint.get_columns())
                commit_t0 = time()
                for single_merge_job in partition_job.single_merge_jobs:
                    timing = {
                        "relname": track.pgclass.name,
                        "n_jobs": 1,
                        "n_lightpoints": 0,
                        "commit_time": 0,
                        "obs_insertion_time": 0,
                        "copy_elapsed": 0
                    }
                    try:
                        lp, lp_timing = self.process_single_merge_job(single_merge_job)
                        if lp is None:
                            self.result_queue.put(timing)
                            continue

                        timing["n_lightpoints"] = len(lp)
                        orbit_info = self.orbit_from_lp(lp)
                        lp_timing = push_lightpoints(mgr, lp)
                        timing.update(lp_timing)
                        obs_timing = push_observations(
                            conn,
                            [(single_merge_job.lightcurve_id, orbit_info["orbit_id"], single_merge_job.camera, single_merge_job.ccd)]
                        )

                        elapsed = time() - commit_t0
                        if elapsed >= self.commit_threshold:
                            conn.commit()
                            timing["commit_time"] = time() - elapsed

                            timing_df = pd.DataFrame(timings)
                            timing_sum = timing_df.sum()

                            commit_t0 = time()
                            self.log(
                                f"Committed after {elapsed} seconds for {timing_sum.n_lightpoints} total lightpoints",
                                level="info"
                            )
                            timings = []
                        else:
                            timings.append(timing)

                    except UniqueViolation:
                        tic_id = single_merge_job.tic_id
                        orbit_number = single_merge_job.orbit_number
                        self.log(f"Duplicate job for {tic_id} on orbit {orbit_number}", level="warning")
                        conn.rollback()
                conn.commit()

    def run(self):
        self.set_name()
        self.log("Initialized")
        self.cached_get_qflags = lru_cache(maxsize=16)(self.get_qflags)
        self.cached_get_correct_mid_tjd = lru_cache(maxsize=16)(
            self.get_correct_mid_tjd
        )
        self.log("Initialized lru h5 readers")
        with db_from_config() as db:
            self.orbit_id_map = dict(
                db
                .query(Frame.cadence, Frame.orbit_id)
                .filter(Frame.frame_type_id == "Raw FFI")
            )
            self.orbit_number_map = dict(
                db
                .query(Frame.cadence, Orbit.orbit_number)
                .join(Frame.orbit)
                .filter(Frame.frame_type_id == "Raw FFI")
            )
            self.log("Queried cadence->orbit relations")

        job = self.job_queue.get()
        self.log("First job obtained")
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
        self.log("Received end-of-work signal")



