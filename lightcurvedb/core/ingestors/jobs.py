import pathlib
from collections import namedtuple
from itertools import product

from click import echo
from loguru import logger
from tqdm import tqdm

from lightcurvedb.experimental.temp_table import TempTable
from lightcurvedb.models import (
    Aperture,
    Lightcurve,
    LightcurveType,
    Observation,
    Orbit,
)
from lightcurvedb.util.contexts import extract_pdo_path_context
from lightcurvedb.util.iter import chunkify

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


def _yield_lightcurve_fields(db, background_name_template="%background%"):
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


def _tic_from_h5(path):
    base = path.name
    return int(base.split(".")[0])


def _get_or_create_lightcurve_id(
    db, id_map, tic_id, aperture, lightcurve_type
):
    """
    Helper method to resolve lightcurve ids. If an ID is not found, a
    lightcurve object is created and sent to the database for a new ID.
    """
    key = (tic_id, aperture, lightcurve_type)

    try:
        id_ = id_map[key]
    except KeyError:
        logger.debug(f"Need new id for {tic_id}: {aperture} {lightcurve_type}")
        lc = Lightcurve(
            tic_id=tic_id,
            aperture_id=aperture,
            lightcurve_type_id=lightcurve_type,
        )
        db.add(lc)
        db.commit()
        id_ = lc.id
        logger.debug(f"{tic_id}: {aperture} {lightcurve_type} assigned {id_}")

    return id_


def _get_smjs_from_paths(db, contexts):
    """
    Given a list of h5 paths, convert each one into the corresponding
    single merge jobs.
    """
    pairs = list(_yield_lightcurve_fields(db))

    tic_ids = set(_tic_from_h5(context["path"]) for context in contexts)
    id_map = {}

    temp = TempTable(db, "temp_tic_ids")
    temp.add_column("tic_id", "bigint", primary_key=True)
    with temp:
        temp.insert_many(tic_ids, scalar=True)
        for aperture, lightcurve_type in pairs:
            logger.debug(
                f"Quering lightcurve ids for {aperture} and {lightcurve_type}"
            )
            q = (
                db.query(Lightcurve.tic_id, Lightcurve.id)
                .join(temp.table, temp["tic_id"] == Lightcurve.tic_id)
                .filter(
                    Lightcurve.aperture_id == aperture,
                    Lightcurve.lightcurve_type_id == lightcurve_type,
                )
            )
            for tic_id, lightcurve_id in q:
                key = (tic_id, aperture, lightcurve_type)
                id_map[key] = lightcurve_id

    jobs = []
    logger.debug("Grabbing ids for each file")
    for context in tqdm(contexts, unit="paths"):
        path = context["path"]
        tic_id = int(_tic_from_h5(path))
        for aperture, lightcurve_type in pairs:
            id_ = _get_or_create_lightcurve_id(
                db, id_map, tic_id, aperture, lightcurve_type
            )

            smj = SingleMergeJob(
                lightcurve_id=id_,
                tic_id=tic_id,
                aperture=aperture,
                lightcurve_type=lightcurve_type,
                orbit_number=int(context["orbit_number"]),
                camera=int(context["camera"]),
                ccd=int(context["ccd"]),
                file_path=str(path),
            )
            jobs.append(smj)
    return jobs


class DirectoryPlan:
    files = None
    jobs = None

    DEFAULT_TIC_CATALOG_TEMPLATE = (
        "/pdo/qlp-data/"
        "orbit-{orbit_number}/"
        "ffi/run/"
        "catalog_{orbit_number}_{camera}_{ccd}_full.txt"
    )

    DEFAULT_QUALITY_FLAG_TEMPLATE = (
        "/pdo/qlp-data/"
        "orbit-{orbit_number}/"
        "ffi/run/"
        "cam{camera}ccd{ccd}_qflag.txt"
    )

    def __init__(
        self,
        directories: list[pathlib.Path],
        db,
        recursive=False,
    ):
        self.source_dirs = directories
        self.recursive = recursive
        self.db = db
        self._look_for_files()
        self._preprocess_files()

    def __repr__(self):
        file_msg = f"Considered {len(self.files)} files"
        tics = {context.tic_id for context in self.jobs}
        tic_msg = f"{len(tics)} total unique TIC ids"

        messages = [file_msg, tic_msg]

        return "\n".join(messages)

    def _look_for_files(self):
        contexts = []
        for source_dir in self.source_dirs:
            logger.debug(f"Looking for h5 files in {source_dir}")
            if self.recursive:
                _file_iter = source_dir.rglob("*.h5")
            else:
                _file_iter = source_dir.glob("*.h5")
            i = 0
            for i, h5_path in enumerate(_file_iter):
                context = extract_pdo_path_context(str(h5_path))
                context["path"] = h5_path
                contexts.append(context)
            logger.debug(f"Found {i} files in {source_dir}")

        self.files = contexts

    def _get_observed(self, db, jobs):
        _mask = set()
        observed = set()

        for job in jobs:
            if job.orbit_number not in _mask:
                logger.debug(
                    f"Querying observation cache for orbit {job.orbit_number}"
                )
                q = (
                    db.query(Observation.lightcurve_id, Orbit.orbit_number)
                    .join(Observation.orbit)
                    .filter(
                        Orbit.orbit_number == job.orbit_number,
                    )
                )
                i = 0
                for i, row in enumerate(q):
                    id_, orbit_number = row
                    observed.add((id_, orbit_number))

                logger.debug(
                    f"Tracking {i} entries from orbit {job.orbit_number}"
                )

                _mask.add(job.orbit_number)

        return observed

    def _preprocess_files(self):
        logger.debug(f"Preprocessing {len(self.files)} files")
        with self.db as db:
            jobs = []
            naive_jobs = _get_smjs_from_paths(db, self.files)
            observed = self._get_observed(db, naive_jobs)

            logger.debug(
                f"Created {len(naive_jobs)} jobs requiring dedup check"
            )
            for job in tqdm(naive_jobs, unit=" jobs"):
                key = (job.lightcurve_id, job.orbit_number)
                if key not in observed:
                    jobs.append(job)
                    observed.add(key)
        ignored = len(naive_jobs) - len(jobs)
        logger.debug(f"Generated {len(jobs)} jobs, ignoring {ignored}")
        self.jobs = jobs

    def _get_unique_observed(self):
        unique_observed = set()
        for job in self.get_jobs():
            key = (job.orbit_number, job.camera, job.ccd)
            unique_observed.add(key)
        return unique_observed

    def get_jobs(self):
        return self.jobs

    def yield_needed_tic_catalogs(self, path_template=None):
        if path_template is None:
            path_template = self.DEFAULT_TIC_CATALOG_TEMPLATE

        for orbit_number, camera, ccd in self._get_unique_observed():
            expected_path = path_template.format(
                orbit_number=orbit_number, camera=camera, ccd=ccd
            )
            yield pathlib.Path(expected_path)

    def yield_needed_quality_flags(self, path_template=None):
        if path_template is None:
            path_template = self.DEFAULT_QUALITY_FLAG_TEMPLATE
        for orbit_number, camera, ccd in self._get_unique_observed():
            expected_path = path_template.format(
                orbit_number=orbit_number, camera=camera, ccd=ccd
            )
            yield pathlib.Path(expected_path), camera, ccd
