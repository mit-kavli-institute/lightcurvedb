from lightcurvedb.core.ingestors.lightcurve_ingestors import (
    allocate_lightcurve_ids,
    get_missing_ids,
)
from lightcurvedb.util.iter import chunkify
from lightcurvedb.core.ingestors.temp_table import FileObservation
from lightcurvedb.models import (
    Observation,
    Lightcurve,
    Lightpoint,
    Orbit,
    Aperture,
    LightcurveType,
)
from collections import namedtuple, defaultdict
from sqlalchemy import distinct, text, func
from sqlalchemy.orm import Bundle
from loguru import logger
from click import echo
from tqdm import tqdm
from itertools import product
import pandas as pd
from math import isnan


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
    "PartitionJob", ("partition_oid", "single_merge_jobs")
)
LightcurveJob = namedtuple(
    "LightcurveJob", ("lightcurve_id", "single_merge_jobs")
)
PHYSICAL_LIMIT = {1, 2, 3, 4}


def apply_physical_filter(filters, attr, tokens):
    """
    For cameras and ccds, if all of them are listed, don't bother with
    a filter. Otherwise construct the filter object and apply it to the
    list of filters.

    Returns
    -------
    List of SQLAlchemy filters
    """
    if set(tokens) == PHYSICAL_LIMIT:
        # No need to filter
        return filters
    return filters + [attr.in_(tokens)]


def sqlite_accumulator(scalars, filter_col, base_q, maxlen=999):
    """
    SQLite has a max limit to how many scalar values can be present when
    comparing a membership filter. Break apart the query using the max lengths
    and accumulate the values in a list.

    Parameters
    ----------
    scalars: iterable
        An iterable of values to check against.
    filter_col: SQLAlchemy column
        The filter column to check membership with.
    base_q: SQLAlchemy query
        A base query to serve as a common entry point.
    Returns
    -------
    list
        A list of the specified results in ``base_q``.
    """
    chunks = list(chunkify(scalars, maxlen))
    accumulator = []
    echo("Chunkifying for {0}".format(base_q))
    for chunk in tqdm(chunks):
        q = base_q.filter(filter_col.in_(chunk))
        accumulator.extend(q)
    return accumulator


def yield_lightcurve_fields(db, background_name_template="%background%"):
    """
    Yield lightcurve apertures and types 
    """
    bg_aperture_filter = Aperture.name.ilike(background_name_template)
    bg_type_filter = LightcurveType.name.ilike(background_name_template)

    fg_apertures = db.query(Aperture).filter(~bg_aperture_filter)
    fg_types = db.query(LightcurveType).filter(~bg_type_filter)

    bg_apertures = db.query(Aperture).filter(bg_aperture_filter)
    bg_types = db.query(LightcurveType).filter(bg_type_filter)

    _iter = product(fg_apertures, fg_types)
    for fg_aperture, fg_lightcurve_type in _iter:
        yield fg_aperture.name, fg_lightcurve_type.name

    _iter = product(bg_apertures, bg_types)
    for bg_aperture, bg_lightcurve_type in _iter:
        yield bg_aperture.name, bg_lightcurve_type.name


class IngestionPlan(object):
    def __init__(
        self,
        db,
        cache,
        full_diff=True,
        orbits=None,
        cameras=None,
        ccds=None,
        tic_mask=None,
        invert_mask=False,
    ):
        echo("Constructing LightcurveDB and Cache queries")

        # Construct relevant TIC ID query from file observations in cache
        cache_filters = []
        current_obs_filters = []
        if orbits:
            cache_filters.append(
                FileObservation.orbit_number.in_(orbits)
            )
            o_ids = [id_ for id_, in db.query(Orbit.id).filter(Orbit.orbit_number.in_(orbits))]
            current_obs_filters.append(
                Observation.orbit_id.in_(o_ids)
            )
        else:
            o_ids = []

        if cameras:
            cache_filters = apply_physical_filter(
                cache_filters,
                FileObservation.camera,
                cameras
            )
            current_obs_filters = apply_physical_filter(
                current_obs_filters,
                Observation.camera,
                cameras
            )

        if ccds:
            cache_filters = apply_physical_filter(
                cache_filters,
                FileObservation.ccd,
                ccds
            )
            current_obs_filters = apply_physical_filter(
                current_obs_filters,
                Observation.ccd,
                ccds
            )

        file_obs_bn = Bundle(
            "c",
            FileObservation.tic_id,
            FileObservation.orbit_number,
            FileObservation.camera,
            FileObservation.ccd,
            FileObservation.file_path
        )
        base_q = cache.query(file_obs_bn)
        echo("Querying file cache")
        if full_diff:
            if tic_mask:
                file_observations = sqlite_accumulator(
                    tic_mask,
                    FileObservation.tic_id,
                    base_q
                )
            else:
                cache_filters = (
                    FileObservation.tic_id.in_(
                        cache.query(FileObservation.tic_id).filter(*cache_filters).subquery()
                    ),
                )
                file_observations = base_q.filter(*cache_filters)
        else:
            file_observations = (
                base_q
                .filter(*cache_filters)
            )

        tic_ids = {file_obs.c.tic_id for file_obs in file_observations}
        self.tics = tic_ids
        db.execute(text("SET LOCAL work_mem TO '2GB'"))

        echo("Performing lightcurve query")
        apertures = [ap.name for ap in db.query(Aperture)]
        lightcurve_types = [lc_t.name for lc_t in db.query(LightcurveType)]

        lc_bn = Bundle(
            "c",
            Lightcurve.tic_id,
            Lightcurve.aperture_id,
            Lightcurve.lightcurve_type_id,
            Lightcurve.id
        )

        lightcurves = (
            db
            .query(lc_bn)
            .filter(Lightcurve.tic_id.in_(tic_ids))
        )

        id_map = {}
        echo("Reading lightcurves for ID mapping")
        for lc in tqdm(lightcurves, unit=" lightcurves"):
            id_map[(lc.c.tic_id, lc.c.aperture_id, lc.c.lightcurve_type_id)] = lc.c.id

        plan = []
        cur_tmp_id = -1

        self.ignored_jobs = 0
        orbit_map = dict(db.query(Orbit.orbit_number, Orbit.id))
        echo("Building job list")
        pairs = list(yield_lightcurve_fields(db))
        for file_obs in tqdm(file_observations, unit=" file observations"):
            orbit_id = orbit_map[file_obs.c.orbit_number]
            for ap, lc_t in pairs:
                lc_key = (file_obs.c.tic_id, ap, lc_t)
                try:
                    id_ = id_map[lc_key]
                except KeyError:
                    id_ = cur_tmp_id
                    id_map[lc_key] = id_
                    cur_tmp_id -= 1

                ingest_job = {
                    "lightcurve_id": id_,
                    "tic_id": file_obs.c.tic_id,
                    "aperture": ap,
                    "lightcurve_type": lc_t,
                    "orbit_number": file_obs.c.orbit_number,
                    "camera": file_obs.c.camera,
                    "ccd": file_obs.c.ccd,
                    "file_path": file_obs.c.file_path,
                }

                plan.append(ingest_job)
        echo("Converting plan to dataframe")
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
            self.ignored_jobs
        )

    def assign_new_lightcurves(self, db, fill_id_gaps=False):
        if len(self._df) == 0:
            # Nothing todo
            return

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

        update_map = dict(zip(new_ids, usable_ids))

        echo("Updating ingestion plan temporary IDs")
        self._df["lightcurve_id"] = self._df["lightcurve_id"].map(lambda id_: update_map.get(id_, id_))

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

    def get_jobs_by_partition(self, db, max_length):
        buckets = defaultdict(list)
        echo("Grabbing partition ranges...")
        if len(self._df) == 0:
            # No duplicates, partition is empty
            return []

        id_oid_map = dict(db.map_values_to_partitions(
            Lightpoint, self._df["lightcurve_id"]
        ))

        echo("Assigning lightcurve id -> table oids")
        tqdm.pandas()
        self._df["oid"] = self._df["lightcurve_id"].progress_apply(lambda id_: int(id_oid_map[id_]))

        echo("Grouping by table oid")
        for row in tqdm(self._df.to_dict("records"), unit=" id rows"):
            oid = row.pop("oid")
            buckets[oid].append(SingleMergeJob(**row))

        partition_jobs = []
        echo("Constructing multiprocessing work")
        with tqdm(total=len(buckets), unit=" partition jobs") as bar:
            for partition_oid, jobs in buckets.items():
                partition_jobs.append(
                    PartitionJob(
                        partition_oid=partition_oid, single_merge_jobs=jobs
                    )
                )
            bar.update(1)
        return partition_jobs

    def get_jobs_by_lightcurve(self, db):
        if len(self._df) == 0:
            return []
        self._df.drop_duplicates(
            subset=["lightcurve_id", "orbit_number"], inplace=True
        )
        bucket = defaultdict(list)
        echo("Grouping by lightcurve id")
        jobs = []
        for job in tqdm(self._df.to_dict("records")):
            bucket[job["lightcurve_id"]].append(SingleMergeJob(**job))

        lightcurve_jobs = []
        for id_, jobs in bucket.items():
            lightcurve_jobs.append(
                LightcurveJob(
                    lightcurve_id=id_, single_merge_jobs=jobs
                )
            )
        return lightcurve_jobs
