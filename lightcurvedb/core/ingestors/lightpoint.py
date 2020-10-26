try:
    # Python agnostic import of Queue
    import queue
except ImportError:
    import Queue as queue

import os
import struct
from collections import defaultdict, namedtuple
from multiprocessing import Process

import numpy as np
import pandas as pd

from lightcurvedb import db_from_config
from lightcurvedb.core.base_model import QLPModel
from lightcurvedb.core.ingestors.lightcurve_ingestors import (
    h5_to_kwargs,
    kwargs_to_df,
    load_lightpoints,
)
from lightcurvedb.models import Lightcurve, Lightpoint, Observation, Orbit
from lightcurvedb.util.logger import lcdb_logger as logger

# from pgcopy import CopyManager
from sqlalchemy import Sequence, Table, bindparam

try:
    from math import isclose
except ImportError:
    # Python 2
    def isclose(x, y):
        return np.isclose([x], [y])[0]


LP_COLS = [
    "lightcurve_id",
    "cadence",
    "barycentric_julian_date",
    "data",
    "error",
    "x_centroid",
    "y_centroid",
    "quality_flag",
]

LP_COL_RENAME = dict(
    cadences="cadence",
    values="data",
    errors="error",
    x_centroids="x_centroid",
    y_centroids="y_centroid",
    quality_flags="quality_flag",
)

DIFF_COLS = ["values", "barycentric_julian_date", "quality_flags"]

MergeJob = namedtuple(
    "MergeJob", ("tic_id", "ra", "dec", "tmag", "file_observations")
)

SingleMergeJob = namedtuple(
    "SingleMerge", ("tic_id", "aperture", "lightcurve_type", "id")
)

PartitionJob = namedtuple(
    "PartitionJob", ("merge_jobs", "observation_map", "tic_parameters")
)


def cadence_map_to_iter(id_cadence_map):
    for id_, cadences in id_cadence_map.items():
        for cadence in cadences:
            yield id_, cadence


def remove_redundant(id_cadence_map, current_lp):
    """
    Drop any (lightcurve_id, cadence) pairs that appear in the database
    to avoid duplication.
    """
    return current_lp.drop(
        cadence_map_to_iter(id_cadence_map), errors="ignore"
    )


class LightpointProcessor(Process):
    def log(self, msg, level="debug"):
        getattr(logger, level)("{0}: {1}".format(self.name, msg))

    def set_name(self):
        self.name = "{0}-{1}".format(self.prefix, os.getpid())


class MassIngestor(LightpointProcessor):
    prefix = "MassIngestor"

    def __init__(
        self,
        lcdb_config,
        quality_flags,
        time_corrector,
        tic_queue,
        mode="ignore",
        **process_kwargs
    ):
        super(MassIngestor, self).__init__(**process_kwargs)
        self.engine_kwargs = dict(
            executemany_mode="values",
            executemany_values_page_size=10000,
            executemany_batch_page_size=500,
        )
        self.sequence = Sequence("lightcurves_id_seq")
        self.config = lcdb_config
        self.tic_queue = tic_queue
        self.mode = mode
        self.q_flags = quality_flags
        self.time_corrector = time_corrector

        self.db = None
        self.cadence_to_orbit_map = {}
        self.orbit_map = {}
        self.observations = []
        self.lp_cache = defaultdict(list)
        self.table_cache = {}

        self.new_ids = set()
        self.id_map = {}

    def update_ids(self, tic):
        for lc in self.db.lightcurves.filter(Lightcurve.tic_id == tic).all():
            key = (lc.tic_id, lc.aperture_id, lc.lightcurve_type_id)
            self.id_map[key] = lc.id

    def get_id(self, tic, aperture, lc_type):
        key = (tic, aperture, lc_type)
        try:
            return self.id_map[key]
        except KeyError:
            self.id_map[key] = self.db.session.execute(self.sequence)
            self.new_ids.add(self.id_map[key])
            return self.id_map[key]

    def merge(self, job):
        self.update_ids(job.tic_id)
        observed_orbits = {
            r
            for r, in self.db.query(Orbit.orbit_number)
            .join(Observation.orbit)
            .filter(Observation.tic_id == job.tic_id)
            .all()
        }
        processed_orbits = set()

        total_len = 0
        for obs in job.file_observations:
            tic, orbit, camera, ccd, file_path = obs
            if orbit in observed_orbits or orbit in processed_orbits:
                self.log("Found duplicate orbit {0}".format(orbit))
                continue
            self.observations.append(
                dict(
                    tic_id=tic,
                    orbit_id=self.orbit_map[orbit],
                    camera=camera,
                    ccd=ccd,
                )
            )
            processed_orbits.add(orbit)
            for kw in h5_to_kwargs(file_path):
                lc_id = self.get_id(
                    tic, kw["aperture_id"], kw["lightcurve_type_id"]
                )
                kw["id"] = lc_id

                # Update quality flags
                idx = [(cadence, camera, ccd) for cadence in kw["cadences"]]
                updated_qflag = self.q_flags.loc[idx]["quality_flags"]
                updated_qflag = updated_qflag.to_numpy()
                kw["quality_flags"] = updated_qflag

                h5_lp = kwargs_to_df(kw, camera=camera)

                # Align data
                mask = h5_lp["quality_flags"] == 0
                good_values = h5_lp.loc[mask]["values"].to_numpy()
                offset = np.nanmedian(good_values) - job.tmag
                h5_lp["values"] = h5_lp["values"] - offset

                # Timecorrect
                corrected_bjd = self.time_corrector.correct_bjd(
                    job.ra, job.dec, h5_lp
                )
                h5_lp["barycentric_julian_date"] = corrected_bjd
                h5_lp.drop(columns=["camera"], inplace=True)
                h5_lp.sort_index(inplace=True)
                h5_lp.reset_index(inplace=True)

                # Orbital data has been corrected for Earth observation
                # and reference
                # Send to the appropriate table
                partition_begin = (lc_id // 1000) * 1000
                partition_end = partition_begin + 1000
                table = "lightpoints_{0}_{1}".format(
                    partition_begin, partition_end
                )

                self.lp_cache[table].append(h5_lp)

                total_len += len(h5_lp)

        self.log(
            "processed {0} with {1} orbits for {2} new lightpoints".format(
                job.tic_id, len(job.file_observations), total_len
            )
        )

    def flush(self):
        """Flush all caches to database"""
        # Insert all new lightcurves
        lcs = []
        for key, id_ in self.id_map.items():
            if id_ not in self.new_ids:
                continue
            tic_id, ap_id, lc_type_id = key
            lcs.append(
                Lightcurve(
                    id=id_,
                    tic_id=tic_id,
                    aperture_id=ap_id,
                    lightcurve_type_id=lc_type_id,
                )
            )

        self.db.session.add_all(lcs)
        self.db.commit()
        points = 0
        for table, lps in self.lp_cache.items():
            try:
                partition = self.table_cache[table]
            except KeyError:
                partition = Table(
                    table,
                    QLPModel.metadata,
                    schema="partitions",
                    autoload=True,
                    autoload_with=self.db.session.bind,
                )
                self.table_cache[table] = partition

            q = partition.insert().values(
                {
                    Lightpoint.lightcurve_id: bindparam("_id"),
                    Lightpoint.bjd: bindparam("bjd"),
                    Lightpoint.cadence: bindparam("cadences"),
                    Lightpoint.data: bindparam("values"),
                    Lightpoint.error: bindparam("errors"),
                    Lightpoint.x_centroid: bindparam("x_centroids"),
                    Lightpoint.y_centroid: bindparam("y_centroids"),
                    Lightpoint.quality_flag: bindparam("quality_flags"),
                }
            )
            for lp in lps:
                df = lp.reset_index().rename(
                    columns={
                        "lightcurve_id": "_id",
                        "barycentric_julian_date": "bjd",
                    }
                )
                self.db.session.execute(q, df.to_dict("records"))
                points += len(df)

        obs_objs = []
        for obs in self.observations:
            o = Observation(**obs)
            obs_objs.append(o)

        self.db.session.add_all(obs_objs)
        self.db.commit()

        # Reset
        self.new_ids = set()
        self.observations = []
        self.lp_cache = defaultdict(list)
        self.log("flushed {0} lightpoints".format(points))

    def run(self):
        self.db = db_from_config(self.config, **self.engine_kwargs).open()
        self.set_name()

        self.orbit_map = {
            orbit_number: orbit_id
            for orbit_number, orbit_id in self.db.query(
                Orbit.orbit_number, Orbit.id
            ).all()
        }
        first_ingestion = True

        try:
            while True:
                if first_ingestion:
                    job = self.tic_queue.get()
                    first_ingestion = False
                else:
                    job = self.tic_queue.get(timeout=30)
                self.merge(job)
                self.tic_queue.task_done()
                self.flush()

        except queue.Empty:
            # Timed out :(
            self.log("TIC queue timed out. Flushing any remaining data")
            if len(self.id_map) > 0:
                self.flush()
        except KeyboardInterrupt:
            self.log("Received interrupt signal, flushing before exiting...")
            self.flush()
        finally:
            # Cleanup!
            self.db.close()


def partition_copier(
    time_corrector,
    quality_flags,
    partition_job,
    destination="/scratch2",
    lightpoint_pattern="lightpoints_{begin_range}.blob",
    obs_pattern="observations_{begin_range}.blob",
):
    """
    Merges and corrects a lightcurve and its source files.
    Returns a multi-index pandas dataframe representing the data
    for all lightcurves and the new observations to update.
    """
    merge_jobs, observation_map, tic_parameters = partition_job
    lps = []
    observations = []

    for tic_id, ap_id, lct_id, id_ in merge_jobs:
        observations = observation_map.loc[tic_id]
        tmag, ra, dec = tic_parameters.loc[tic_id]
        logger.info(
            "processing {0} [{1} {2}:{3}]".format(
                tic_id,
                tmag,
                ra,
                dec,
            )
        )

        for orbit, camera, ccd, path in observations.to_records():
            lightpoints = load_lightpoints(path, id_, ap_id, lct_id)

            # Update quality flags
            idx = lightpoints[["cadence", "camera", "ccd"]]
            lightpoints["quality_flag"] = quality_flags.loc[idx].to_numpy()

            # Align data
            mask = lightpoints["quality_flag"] == 0
            good_values = lightpoints.loc[mask]["data"].to_numpy()
            offset = np.nanmedian(good_values) - tmag
            lightpoints["data"] = lightpoints["data"] - offset

            # Time correct
            bjd = time_corrector.correct_bjd(ra, dec, lightpoints)
            lightpoints["barycentric_julian_date"] = bjd

            # All lightcurve information has been filtered for
            # full sector analysis.
            lps.append(lightpoints)
            observations.append(
                dict(tic_id=tic_id, orbit_number=orbit, camera=camera, ccd=ccd)
            )
    logger.debug("performing cadence de-duplication and pre-ordering")

    # Concat full lightcurve and remove duplicate cadences
    full_lp = pd.concat(lps).set_index("lightcurve_id", "cadence").sort_index()
    full_lp = full_lp[~full_lp.index.duplicated(keep="last")]

    lp_filename = lightpoint_pattern.format(
        begin_range=(merge_jobs[0][3] // 1000) * 1000
    )
    obs_filename = obs_pattern.format(
        begin_range=(merge_jobs[0][3] // 1000) * 1000
    )

    # Write lightpoints
    lp_path = os.path.join(destination, lp_filename)
    obs_path = os.path.join(destination, obs_filename)

    logger.debug(
        "finished processing {0} jobs, dumping to {1} and {2}".format(
            len(merge_jobs), lp_path, obs_path
        )
    )

    lp_packer = struct.Struct(Lightpoint.struct_pattern)
    obs_packer = struct.Struct("QccH")

    with open(lp_path, "wb") as lp_out:
        for record in full_lp.to_records():
            packed = lp_packer.pack(*record)
            lp_out.write(packed)

    with open(obs_path, "wb") as obs_out:
        for record in observations:
            packed = obs_packer(
                record["tic_id"],
                record["camera"],
                record["ccd"],
                record["orbit_number"],
            )
            obs_out.write(packed)

    return lp_out, obs_out


def partition_consumer(config, lightpoint_filepath, observation_filepath):
    pass
