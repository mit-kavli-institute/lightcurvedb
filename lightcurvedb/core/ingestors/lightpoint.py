try:
    # Python agnostic import of Queue
    import queue
except ImportError:
    import Queue as queue

import os
import struct
from click import echo, style
from multiprocessing import Process
from time import sleep, time
from itertools import product
from functools import partial

import numpy as np
import pandas as pd

from lightcurvedb import db_from_config
from lightcurvedb.core.base_model import QLPModel
from collections import namedtuple, defaultdict
from lightcurvedb.models import (
    Lightcurve,
    Lightpoint,
    Orbit,
    Observation,
    Aperture,
    LightcurveType,
)
from lightcurvedb.core.ingestors.cache import IngestionCache
from lightcurvedb.core.ingestors.lightcurve_ingestors import (
    h5_to_kwargs,
    kwargs_to_df,
    load_lightpoints,
    get_missing_ids,
    allocate_lightcurve_ids,
)
from lightcurvedb.core.ingestors.temp_table import FileObservation
from lightcurvedb.core.tic8 import one_off
from lightcurvedb.legacy.timecorrect import PartitionTimeCorrector
from lightcurvedb.core.datastructures.data_packers import (
    LightpointPartitionWriter,
    LightpointPartitionReader,
)
from lightcurvedb.util.logger import lcdb_logger as logger

from lightcurvedb import db_from_config
from sqlalchemy import bindparam, Sequence, Table, text
from tqdm import tqdm
from multiprocessing import Process, Pool
from pgcopy import CopyManager

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

LP_COL_RENAME = {
    "cadences": "cadence",
    "values": "data",
    "errors": "error",
    "x_centroids": "x_centroid",
    "y_centroids": "y_centroid",
    "quality_flags": "quality_flag",
}

DIFF_COLS = ["values", "barycentric_julian_date", "quality_flags"]

MergeJob = namedtuple(
    "MergeJob", ("tic_id", "ra", "dec", "tmag", "file_observations")
)

SingleMergeJob = namedtuple(
    "SingleMergeJob", ("tic_id", "aperture", "lightcurve_type", "id", "files")
)

PartitionJob = namedtuple(
    "PartitionJob", ("partition_start", "partition_end", "merge_jobs")
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
    tics = list(file_df.index)
    missing_ids = []

    if bar:
        progress = bar(total=len(file_df) * len(apertures) * len(types))

    for tic_id, orbit_number, _ in file_df.to_records():
        if (tic_id, orbit_number) in already_observed:
            yield None
            if bar:
                progress.update(len(apertures) * len(types))
            continue
        files = list(file_df.loc[tic_id])
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
                missing_ids.append((tic_id, ap, lc_t))
            finally:
                if bar:
                    progress.update(1)

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

        n_still_missing = len(usable_ids) - n_required
        usable_ids.update(allocate_lightcurve_ids(db, n_still_missing))
        values_to_insert = []
        echo("Creating jobs using queried ids")
        progress = bar(total=len(missing_ids))
        for id_, params in zip(usable_ids, missing_ids):
            job = SingleMergeJob(
                id=id_,
                tic_id=params[0],
                aperture=params[1],
                lightcurve_type=params[2],
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
        # update db
        echo("Submitting new lightcurve parameters to database")
        db.session.bulk_insert_mappings(Lightcurve, values_to_insert)
        db.commit()


class LightpointNormalizer(object):
    def __init__(self, cache, db):
        self.time_corrector = PartitionTimeCorrector(db)
        self.quality_flags = cache.quality_flag_df.reset_index()
        self.quality_flags.rename(
            mapper={"cadences": "cadence", "quality_flags": "new_qflag"},
            inplace=True,
            axis=1,
        )

        self.stellar_params = cache.tic_parameter_df

        orbits = db.query(Orbit.orbit_number, Orbit.id)
        self.orbit_map = dict(orbits.all())

    def get_stellar_params(self, tic_id):
        try:
            params = self.stellar_params.loc[tic_id]
        except KeyError:
            params = one_off(tic_id, "tmag", "ra", "dec")
        return params

    def correct(self, tic_id, lightpoint_df):
        # Grab relevant data
        tmag, ra, dec = self.get_stellar_params(tic_id)

        # Assign new quality_flags
        joined = lightpoint_df.merge(
            self.quality_flags, on=["cadence", "camera", "ccd"]
        )

        lightpoint_df["quality_flag"] = joined["new_qflag"]

        # Perform orbital aligment
        mask = lightpoint_df["quality_flag"] == 0
        good_values = lightpoint_df[mask]["data"].to_numpy()
        offset = np.nanmedian(good_values) - tmag
        lightpoint_df["data"] = lightpoint_df["data"] - offset

        # Time correct
        bjd = self.time_corrector.correct_bjd(ra, dec, lightpoint_df)
        try:
            lightpoint_df["barycentric_julian_date"] = bjd
        except ValueError:
            logger.debug("Error correcting for BJD")
            raise RuntimeError(
                "Error processing\n{0}, got bjd array len {1}".format(
                    lightpoint_df, len(bjd)
                )
            )

        return lightpoint_df


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
    def log(self, msg, level="debug", exc_info=False):
        getattr(logger, level)(
            "{0}: {1}".format(self.name, msg), exc_info=exc_info
        )

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
        self.engine_kwargs = {
            "executemany_mode": "values",
            "executemany_values_page_size": 10000,
            "executemany_batch_page_size": 500,
        }
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

            if not os.path.exists(file_path):
                self.log(
                    "Could not find file {0}!".format(file_path), level="error"
                )
                continue

            if orbit in observed_orbits or orbit in processed_orbits:
                self.log("Found duplicate orbit {0}".format(orbit))
                continue
            self.observations.append(
                {
                    "tic_id": tic,
                    "orbit_id": self.orbit_map[orbit],
                    "camera": camera,
                    "ccd": ccd,
                }
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
                    job = self.tic_queue.get(timeout=3000)
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


class PartitionMerger(LightpointProcessor):
    prefix = "PartitionMerger"

    lp_cols = Lightpoint.get_columns()

    def __init__(self, corrector, partition_queue, **kwargs):
        self.scratch_dir = kwargs.pop("scratch_dir", "/scratch2")
        self.submit = kwargs.pop("submit", True)

        super(PartitionMerger, self).__init__(**kwargs)
        self.corrector = corrector
        self.partition_queue = partition_queue

    def merge(self, job):
        lps = []
        self.log(
            "Processing partition {0} with {1} lightcurves".format(
                job.partition_start, len(job.merge_jobs)
            )
        )
        for merge_job in job.merge_jobs:
            self.log(
                "Processing {0} {1} {2} with {3} files".format(
                    merge_job.tic_id,
                    merge_job.aperture,
                    merge_job.lightcurve_type,
                    len(merge_job.files),
                )
            )
            for h5 in merge_job.files:
                self.log("Parsing {0}".format(h5), level="trace")
                try:
                    lp = load_lightpoints(
                        h5,
                        merge_job.id,
                        merge_job.aperture,
                        merge_job.lightcurve_type,
                    )
                    lp = self.corrector.correct(merge_job.tic_id, lp)
                    lps.append(lp)
                except OSError:
                    self.log("could not find {0}".format(h5))
        if not lps:
            self.log("Found no valid jobs to make partition...")
            return None

        raw_partition = pd.concat(lps)

        observations = raw_partition[
            ["tic_id", "orbit_number", "camera", "ccd"]
        ].set_index(["tic_id", "orbit_number"])
        observations.sort_index(inplace=True)
        observations = observations[
            ~observations.index.duplicated(keep="last")
        ]
        observations = observations.sort_index().reset_index()

        observations["orbit_id"] = observations.apply(
            lambda row: self.orbit_map[row["orbit_number"]], axis=1
        )

        writer.add_observations(observations.to_dict("records"))
        lc = raw_partition[list(self.lp_cols)]
        lc.set_index(["lightcurve_id", "cadence"], inplace=True)
        lc = lc.sort_index()
        lc = lc[~lc.index.duplicated(keep="last")]

        writer.add_lightpoints(lc)
        writer.write()
        self.log("wrote to {0}".format(writer.blob_path))
        return writer.blob_path

    def run(self):
        self.set_name()
        while True:
            try:
                job = self.partition_queue.get(block=True)
                if job is None:
                    self.log("received end-of-work signal")
                    self.partition_queue.task_done()
                    break

                partition_blob_path = self.merge(job)

                if partition_blob_path is None:
                    self.log("attempted to put empty partition...")
                    self.partition_queue.task_done()
                    continue

                if not self.submit:
                    continue

                self.log(
                    "submitting partition blob {0}".format(partition_blob_path)
                )
                self.ingestion_queue.put(partition_blob_path)
                self.partition_queue.task_done()
            except ConnectionResetError as e:
                # Queues were reset at one point
                max_tries = 10
                while max_tries >= 0:
                    try:
                        sleep(0.5)
                        self.ingestion_queue.put(partition_blob_path)
                        # successfully emplaced
                        break
                    except ConnectionResetError:
                        max_tries -= 1
                        self.log(
                            "Still unable to connect to queue, "
                            "{0} tries remaining".format(max_tries)
                        )
                if max_tries <= 0:
                    # Unable to resolveproblem
                    self.log(
                        "could not connect to queue {0}, exiting".format(
                            self.ingestion_queue
                        ),
                        level="error",
                    )
                    break
            except Exception as e:
                self.log(e, level="error", exc_info=True)
                break

        self.log("Done!")


class PartitionConsumer(LightpointProcessor):
    prefix = "PartitionConsumer"

    def __init__(self, config, blob_queue, **kwargs):
        self.run_truncate = kwargs.pop("truncate", False)
        super(PartitionConsumer, self).__init__(**kwargs)
        self.config = config
        self.blob_queue = blob_queue

    def copy(self, path):
        self.log("Processing {0}".format(path))
        reader = LightpointPartitionReader(path)

        with db_from_config(self.config) as db:

            preamble = reader.preamble
            if self.run_truncate:
                partition_name = reader.partition_name
                TRUNCATE = text(
                    "TRUNCATE partitions.{0}".format(partition_name)
                )
                tic_ids = reader.get_tic_ids()
                orbit_numbers = reader.get_orbit_numbers(db)

                self.log("Deleting and running truncate")

                db.query(Observation).filter(
                    Orbit.id == Observation.orbit_id,
                    Observation.tic_id.in_(tic_ids),
                ).delete(synchronize_session=False)
                db.session.execute(TRUNCATE)

            self.log(
                "Copying {0} lightpoints".format(
                    preamble["number_of_lightpoints"]
                )
            )
            reader.upload(db)

        os.remove(path)

    def run(self):
        self.set_name()
        while True:
            try:
                path = self.blob_queue.get()
            except ConnectionResetError:
                max_tries = 10
                while max_tries >= 0:
                    try:
                        sleep(0.5)
                        path = self.blob_queue.get(block=True)
                        # successfully grabbed queue
                        break
                    except ConnectionResetError:
                        max_tries -= 1
                        self.log(
                            "Still unable to connect to queue, "
                            "{0} tries remaining".format(max_tries)
                        )
                if max_tries <= 0:
                    # Unable to resolve problem
                    self.log(
                        "could not connect to queue {0}, exciting".format(
                            self.blob_queue
                        ),
                        level="error",
                    )
                    break
            except Exception as e:
                self.log(e, level="error", exc_info=True)

            if path is None:
                break
            self.copy(path)
            self.blob_queue.task_done()
            self.log("Done with {0}".format(path))
        self.log("Exiting")


def partition_copier(
    args, destination="/scratch2",
):
    """
    Merges and corrects a lightcurve and its source files.
    Returns a multi-index pandas dataframe representing the data
    for all lightcurves and the new observations to update.
    """
    time_corrector = args[0]
    quality_flags = args[1]
    orbit_map = args[2]
    partition_job = args[3]

    start, end, merge_jobs, observation_map, tic_parameters = partition_job
    writer = LightpointPartitionWriter(start, end, destination)
    merge_jobs.reset_index(inplace=True)
    logger.debug("merger given {0} jobs".format(len(merge_jobs)))

    for job_kwarg in merge_jobs.to_dict("records"):
        tic_id = job_kwarg["tic_id"]
        ap_id = job_kwarg["aperture"]
        lct_id = job_kwarg["lightcurve_type"]
        id_ = job_kwarg["id"]
        try:
            relevant_obs = observation_map.loc[[tic_id]]
        except KeyError:
            continue

        relevant_obs = relevant_obs[
            ["orbit_number", "camera", "ccd", "file_path"]
        ]
        tmag, ra, dec = tic_parameters.loc[tic_id]
        logger.info(
            "processing {0} [{1} {2}:{3}] with {4} files".format(
                tic_id, tmag, ra, dec, len(relevant_obs),
            )
        )

        lps = []
        observations = []

        for _, orbit, camera, ccd, path in relevant_obs.to_records():
            try:
                lightpoints = load_lightpoints(path, id_, ap_id, lct_id)
            except OSError:
                continue

            # Update quality flags
            joined = lightpoints.merge(
                quality_flags, on=["cadence", "camera", "ccd"]
            )

            lightpoints["quality_flag"] = joined["new_qflags"]

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
        if len(lps) == 0:
            continue

        # Full orbital lightcurve
        # Concat full lightcurve and remove duplicate cadences
        full_lp = (
            pd.concat(lps).set_index("lightcurve_id", "cadence").sort_index()
        )
        full_obs = pd.DataFrame(observations)
        full_obs["orbit_id"] = full_obs.apply(
            lambda row: orbit_map[row["orbit_number"]], axis=1
        )
        full_obs.drop("orbit_number", inplace=True, axis=1)

        writer.add_observations(full_obs.reset_index().to_dict("records"))
        writer.add_lightpoints(full_lp)

    writer.write()
    return writer.blob_path


def partition_consumer(config, partition_path, id_cadence_blacklist=None):

    blacklist = id_cadence_blacklist if id_cadence_blacklist else {}
    reader = LightpointPartitionReader(partition_path)

    lightpoints = reader.yield_lightpoints()
    sorted_lps = sorted(lightpoints, key=lambda lp: (lp[0], lp[1]))

    obs_params = map(
        lambda row: {
            "tic_id": row[0],
            "camera": row[1],
            "ccd": row[2],
            "orbit_id": row[3],
        },
        reader.yield_observations(),
    )

    with db_from_config(config) as db:
        logger.debug("PartitionConsumer connected to psql...")
        mgr = CopyManager(
            db.session.connection().connection,
            "partitions.{0}".format(reader.partition_name),
            Lightpoint.columns,
        )

        logger.debug("Copying lightpoints")
        mgr.threading_copy(sorted_lps)

        logger.debug("Upserting new observation rows")
        q = Observation.upsert_q()

        db.session.execute(q, list(obs_params))
        db.commit()


def copy_lightpoints(config, corrector, merge_jobs, commit=True):
    lps = []
    start = time()
    n_files = 0
    missed_filepaths = []
    for merge_job in merge_jobs:
        for h5 in merge_job.files:
            try:
                lp = load_lightpoints(
                    h5,
                    merge_job.id,
                    merge_job.aperture,
                    merge_job.lightcurve_type,
                )
                lp = corrector.correct(merge_job.tic_id, lp)
                lps.append(lp)
                n_files += 1
            except OSError:
                missed_filepaths.append(h5)
                continue

    merge_elapsed = time() - start

    if not lps:
        return {
            "status": "ERROR",
            "n_files": len(merge_job.files),
            "missed_files": missed_filepaths,
        }

    # Establish full datastructures for partition and observation
    # updates
    start = time()
    raw_partition = pd.concat(lps)
    lp = raw_partition[list(Lightpoint.get_columns())]
    observations = raw_partition[["tic_id", "orbit_number", "camera", "ccd"]]
    lp = lp.set_index(["lightcurve_id", "cadence"])
    lp.sort_index(inplace=True)

    observations = observations.set_index(["tic_id", "orbit_number"])
    observations.sort_index(inplace=True)

    # Remove any duplication
    lp = lp[~raw_partition.index.duplicated(keep="last")]
    obs = observations[~observations.index.duplicated(keep="last")]
    obs.reset_index(inplace=True)

    obs["orbit_id"] = obs.apply(
        lambda row: corrector.orbit_map[row["orbit_number"]], axis=1
    )
    obs.drop(columns="orbit_number", inplace=True)

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
        copy_elapsed = time() - start

        q = Observation.upsert_q()

        start = time()
        db.session.execute(q, list(obs.to_dict("records")))

        upsert_elapsed = time() - start
        start = time()
        if commit:
            db.commit()
        else:
            db.rollback()
        commit_elapsed = time() - start

    return {
        "status": "OK",
        "n_files": len(merge_job.files),
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
        Return a sequence of NamedTuples describing needed information for ingest.
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
        obs_q = (
            db.query(Observation.tic_id, Orbit.orbit_number)
            .join(Observation.orbit)
            .filter(*obs_clause)
        )
        apertures = [ap.name for ap in db.query(Aperture)]
        types = [t.name for t in db.query(LightcurveType)]
        already_observed = set(obs_q)

        echo("Preparing lightcurve id map")
        lcs = db.lightcurves.filter(
            Lightcurve.tic_id.in_(
                db.query(Observation.tic_id)
                .join(Observation.orbit)
                .filter(*obs_clause)
                .distinct()
                .subquery()
            )
        )
        lc_id_map = {
            (lc.tic_id, lc.aperture_id, lc.lightcurve_type_id): lc.id
            for lc in lcs.yield_per(1000)
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
    echo("{0} partitions will be affected".format(len(bucket)))

    with db_from_config(config) as db:
        cache = IngestionCache()
        normalizer = LightpointNormalizer(cache, db)
        cache.session.close()

    func = partial(copy_lightpoints, db._config, normalizer, commit=commit)

    total_jobs = len(bucket)

    err = style("ERROR", bg="red", blink=True)
    ok = style("OK", fg="green", bold=True)

    error_msg = (
        "{0}: Could not open {{0}} files. " "List written to {{1}}".format(err)
    )
    ok_msg = (
        "{0}: Copied {{0}} files. "
        "Merge time {{1}}s. "
        "Validation time {{2}}s. "
        "Copy time {{3}}s".format(ok)
    )
    all_results = []
    echo("Sending work to {0} processes".format(n_processes))

    with Pool(n_processes) as pool:
        results = pool.imap_unordered(func, bucket.values())
        if tqdm_bar:
            bar = tqdm(total=total_jobs)
        for r in results:
            if r["missed_files"]:
                with open("./missed_merges.txt", "at") as out:
                    out.write("\n".join(r["missed_files"]))

            if r["status"] == "ERROR":
                path = os.path.abspath("./missed_merges.txt")
                msg = error_msg.format(len(r["missed_files"]), path)
            elif r["status"] == "OK":
                msg = ok_msg.format(
                    len("n_files"),
                    r["merge_elapsed"],
                    r["validation_elapsed"],
                    r["copy_elapsed"],
                )
            all_results.append(r)
            if tqdm_bar:
                bar.write(msg)
                bar.update(1)
            else:
                print(msg)
