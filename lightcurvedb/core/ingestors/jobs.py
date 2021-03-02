from lightcurvedb.core.ingestors.lightcurve_ingestors import (
    allocate_lightcurve_ids,
)
from lightcurvedb.core.ingestors.temp_table import FileObservation
from lightcurvedb.models import (
    Observation,
    Lightcurve,
    Orbit,
    Aperture,
    LightcurveType,
)
from lightcurvedb.util.iter import chunkify
from collections import namedtuple, defaultdict
from sqlalchemy import distinct
from sqlalchemy.orm import joinedload
from click import echo
from tqdm import tqdm
from itertools import product
import pandas as pd


SingleMergeJob = namedtuple(
    "SingleMergeJob",
    (
        "lightcurve_id",
        "tic_id",
        "aperture",
        "lightcurve_type",
        "orbit_number",
        "camera",
        "ccd",
        "file_path",
    ),
)

PartitionJob = namedtuple(
    "PartitionJob", ("partition_relname", "single_merge_jobs")
)


class IngestionPlan(object):
    def __init__(
        self,
        db,
        cache,
        orbits=None,
        cameras=None,
        ccds=None,
        tic_mask=None,
        invert_mask=False,
    ):
        echo("Constructing LightcurveDB and Cache queries")

        cache_subquery = cache.query(distinct(FileObservation.tic_id))
        db_lc_query = db.query(Lightcurve).join(Lightcurve.observations)

        if orbits:
            db_lc_query = db_lc_query.join(Observation.orbit).filter(
                Orbit.orbit_number.in_(orbits)
            )
            cache_subquery = cache_subquery.filter(
                FileObservation.orbit_number.in_(orbits)
            )

        if cameras:
            db_lc_query = db_lc_query.filter(Observation.camera.in_(cameras))
            cache_subquery = cache_subquery.filter(
                FileObservation.camera.in_(cameras)
            )

        if ccds:
            db_lc_query = db_lc_query.filter(Observation.ccd.in_(ccds))
            cache_subquery = cache_subquery.filter(
                FileObservation.ccd.in_(ccds)
            )

        if tic_mask:
            db_tic_filter = (
                ~Lightcurve.tic_id.in_(tic_mask)
                if invert_mask
                else Lightcurve.tic_id.in_(tic_mask)
            )
            cache_tic_filter = (
                ~FileObservation.tic_id.in_(tic_mask)
                if invert_mask
                else FileObservation.tic_id.in_(tic_mask)
            )
            db_lc_query = db_subquery.filter(db_tic_filter)
            cache_subquery = cache_subquery.filter(cache_tic_filter)

        echo("Querying file cache")
        file_observations = (
            cache.query(FileObservation)
            .filter(FileObservation.tic_id.in_(cache_subquery.subquery()))
            .all()
        )
        tic_ids = {file_obs.tic_id for file_obs in file_observations}
        relevant_orbit_check = {
            file_obs.orbit_number for file_obs in file_observations
        }

        echo("Getting current observations from database")
        orbit_map = dict(db.query(Orbit.orbit_number, Orbit.id))
        current_obs_q = (
            db.query(Observation.lightcurve_id, Observation.orbit_id)
            .join(Observation.lightcurve)
            .filter(Lightcurve.tic_id.in_(tic_ids),)
        )

        seen_cache = set(tqdm(current_obs_q, unit=" observations"))

        echo("Performing lightcurve query")
        apertures = [ap.name for ap in db.query(Aperture)]
        lightcurve_types = [lc_t.name for lc_t in db.query(LightcurveType)]

        lightcurves = db.query(Lightcurve)
        lightcurves = lightcurves.filter(Lightcurve.tic_id.in_(tic_ids))

        id_map = {}
        echo("Reading lightcurves for existing observations and ID mapping")
        for lc in tqdm(lightcurves, unit=" lightcurves"):
            id_map[(lc.tic_id, lc.aperture_id, lc.lightcurve_type_id)] = lc.id

        plan = []
        cur_tmp_id = -1

        self.ignored_jobs = 0

        echo("Building job list")
        for file_obs in tqdm(file_observations):
            orbit_id = orbit_map[file_obs.orbit_number]
            for ap, lc_t in product(apertures, lightcurve_types):
                lc_key = (file_obs.tic_id, ap, lc_t)
                try:
                    id_ = id_map[lc_key]
                except KeyError:
                    id_ = cur_tmp_id
                    id_map[lc_key] = id_
                    cur_tmp_id -= 1

                if (id_, orbit_id) in seen_cache:
                    self.ignored_jobs += 1
                    continue

                ingest_job = {
                    "lightcurve_id": id_,
                    "tic_id": file_obs.tic_id,
                    "aperture": ap,
                    "lightcurve_type": lc_t,
                    "orbit_number": file_obs.orbit_number,
                    "camera": file_obs.camera,
                    "ccd": file_obs.ccd,
                    "file_path": file_obs.file_path,
                }
                seen_cache.add((id_, orbit_id))

                plan.append(ingest_job)
        self._df = pd.DataFrame(plan)

    def __repr__(self):
        fmt = """
        === Ingestion Plan ========================
        Total TICs       = {0}
        Total jobs       = {1}
        Total partitions = {2}
        -------------------------------------------
        Total existing lightcurves =     {3}
        Total new lightcurves =          {4}
        Total Ignored Jobs (Duplicate) = {5}
        ===========================================
        # TICs split across cameras = {6}
        """
        if len(self._df) == 0:
            return "No Ingestion Plan. Everything looks to be in order."

        partitions = set(self._df.lightcurve_id.apply(lambda x: x // 1000))

        return fmt.format(
            len(set(self._df.tic_id)),
            len(self._df),
            len(partitions),
            len(set(self._df[self._df.lightcurve_id > 0].lightcurve_id)),
            len(set(self._df[self._df.lightcurve_id < 0].lightcurve_id)),
            self.ignored_jobs,
            len(self.targets_across_many_cameras),
        )

    def assign_new_lightcurves(self, db, fill_id_gaps=False):
        new_jobs = self._df[self._df.lightcurve_id < 0]
        new_ids = set(new_jobs.lightcurve_id)

        if len(new_ids) == 0:
            echo("No temporary lightcurve ids found")
            return

        echo("Need to allocate {0} lightcurve ids".format(len(new_ids)))
        if fill_id_gaps:
            echo("Attempting to find gaps in lightcurve id sequence")
            usable_ids = get_missing_ids(db)
            echo("\tObtained {0} usable ids".format(len(usable_ids)))
        else:
            usable_ids = set()

        n_still_missing = len(new_ids) - len(usable_ids)
        n_still_missing = n_still_missing if n_still_missing >= 0 else 0
        usable_ids.update(allocate_lightcurve_ids(db, n_still_missing))
        values_to_insert = []

        update_map = dict(zip(new_ids, usable_ids))

        echo("Updating ingestion plan temporary IDs")
        self._df["lightcurve_id"] = self._df["lightcurve_id"].map(update_map)

        echo("Submitting new lightcurve definitions to database")
        param_df = self._df[self._df.lightcurve_id.isin(usable_ids)]

        param_df = param_df[
            ["lightcurve_id", "tic_id", "aperture", "lightcurve_type"]
        ]
        param_df.rename(
            columns={
                "lightcurve_id": "id",
                "aperture": "aperture_id",
                "lightcurve_type": "lightcurve_type_id",
            },
            inplace=True,
        )

        db.session.bulk_insert_mappings(
            Lightcurve, param_df.drop_duplicates().to_dict("records")
        )
        db.commit()
        echo("Committed new lightcurve definitions")

    @property
    def targets_across_many_cameras(self):
        """
        Return Lightcurve IDs that are found crossing multiple cameras per
        orbit.
        Targets can appear across multiple cameras due to slight overlap in
        camera optics.

        Returns
        -------
        list
            A list of tuples containing (Lightcurve.id, Orbit.orbit_number) to
            represent which Lightcurve in which orbit was found to cross
            multiple cameras.
        """

        split_df = self._df.groupby(["lightcurve_id", "orbit_number"]).filter(
            lambda g: len(g) > 1
        )
        return split_df

    def get_jobs_by_partition(self):
        self._df.drop_duplicates(
            subset=["lightcurve_id", "orbit_number"], inplace=True
        )
        buckets = defaultdict(list)

        for idx, row in self._df.iterrows():
            partition_start = (row["lightcurve_id"] // 1000) * 1000
            partition_end = partition_start + 1000
            relname = "partitions.lightpoints_{0}_{1}".format(
                partition_start, partition_end
            )
            buckets[relname].append(SingleMergeJob(**row))

        partition_jobs = []
        for partition_relname, jobs in buckets.items():
            partition_jobs.append(
                PartitionJob(
                    partition_relname=partition_relname, single_merge_jobs=jobs
                )
            )
        return partition_jobs