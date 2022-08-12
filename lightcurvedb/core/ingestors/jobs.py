import pathlib
from collections import defaultdict
from dataclasses import dataclass
from itertools import product
from typing import List

from click import echo
from loguru import logger
from sqlalchemy import sql
from tqdm import tqdm

from lightcurvedb.models import Aperture, LightcurveType, OrbitLightcurve
from lightcurvedb.util.contexts import extract_pdo_path_context
from lightcurvedb.util.iter import chunkify


@dataclass
class OrbitLightcurveJob:
    tic_id: int
    camera: int
    ccd: int
    orbit_number: int
    aperture: str
    lightcurve_type: str
    file_path: str

    def as_key(self):
        return (
            self.tic_id,
            self.camera,
            self.ccd,
            self.orbit_number,
            self.aperture,
            self.lightcurve_type,
        )


@dataclass
class H5Job:
    file_path: str
    tic_id: int
    orbit_lightcurve_jobs: List[OrbitLightcurveJob]


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


def get_orbit_jobs_from_paths(db, contexts):
    """
    Given a list of h5 paths, convert each one into the corresponding
    single merge jobs.
    """
    pairs = list(_yield_lightcurve_fields(db))
    jobs = []
    for context in tqdm(contexts, unit="paths"):
        path = context["path"]
        tic_id = int(_tic_from_h5(path))
        for aperture, lightcurve_type in pairs:
            job = OrbitLightcurveJob(
                tic_id=tic_id,
                camera=int(context["camera"]),
                ccd=int(context["ccd"]),
                orbit_number=int(context["orbit_number"]),
                aperture=aperture,
                lightcurve_type=lightcurve_type,
                file_path=path,
            )
            jobs.append(job)
    return jobs


def get_observed_from_path(db, path):
    required_contexts = (
        "tic_id",
        "camera",
        "ccd",
        "orbit_id",
        "aperture_id",
        "lightcurve_type_id",
    )
    path_context = extract_pdo_path_context(path)
    constants_from_path = []

    if "orbit_number" in path_context:
        orbit_number = path_context.pop("orbit_number")
        constants_from_path.append(
            OrbitLightcurve.orbit_id == db.get_orbit_id(orbit_number)
        )
    columns = [OrbitLightcurve.id]
    for context in required_contexts:
        try:
            column = sql.expression.literal_column(path_context[context])
            constants_from_path.append(
                getattr(OrbitLightcurve, context) == path_context[context]
            )
        except KeyError:
            column = getattr(OrbitLightcurve, context)
        columns.append(column)

    q = db.query(*columns).filter(*constants_from_path)
    logger.debug(f"Getting observations with {str(q)}")
    return q.all()


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

    def _get_observed(self, db):
        observed = set()
        for source_dir in self.source_dirs:
            logger.debug(f"Interpreting {source_dir} for observations")
            seen_in_dir = get_observed_from_path(db, source_dir)
            logger.debug(f"Found {len(seen_in_dir)} current observations")
            observed.update(seen_in_dir)

        return observed

    def _preprocess_files(self):
        logger.debug(f"Preprocessing {len(self.files)} files")
        buckets = defaultdict(list)
        with self.db as db:
            naive_jobs = get_orbit_jobs_from_paths(db, self.files)
            observed = self._get_observed(db)

            logger.debug(
                f"Created {len(naive_jobs)} jobs requiring dedup check"
            )
            n_accepted = 0
            for job in tqdm(naive_jobs, unit=" jobs"):
                key = job.as_key()
                if key not in observed:
                    buckets[job.file_path].append(job)
                    observed.add(key)
                    n_accepted += 1

        ignored = len(naive_jobs) - n_accepted
        logger.debug(f"Generated {len(naive_jobs)} jobs, ignoring {ignored}")
        jobs = []
        for file_path, orbit_jobs in buckets.items():
            job = H5Job(
                file_path=file_path,
                tic_id=orbit_jobs[0].tic_id,
                orbit_lightcurve_jobs=orbit_jobs,
            )
            jobs.append(job)

        self.jobs = jobs

    def _get_unique_observed(self):
        unique_observed = set()
        for job in self.get_jobs():
            for smj in job.orbit_lightcurve_jobs:
                key = (smj.orbit_number, smj.camera, smj.ccd)
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
