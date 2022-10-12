import pathlib
from collections import Counter, defaultdict
from dataclasses import dataclass
from functools import partial
from itertools import chain, product
from multiprocessing import Pool
from typing import List, Optional

import pandas as pd
import sqlalchemy as sa
from click import echo
from loguru import logger
from tqdm import tqdm

from lightcurvedb.models import (
    Aperture,
    ArrayOrbitLightcurve,
    LightcurveType,
    Orbit,
    OrbitLightcurve,
)
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

    preassigned_id: Optional[int] = None

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
class EM2_H5_Job:
    file_path: pathlib.Path
    tic_id: int
    camera: int
    ccd: int
    orbit_number: int

    @classmethod
    def from_path_context(cls, context):
        job = cls(
            file_path=pathlib.Path(context["path"]),
            tic_id=int(context["tic_id"]),
            camera=int(context["camera"]),
            ccd=int(context["ccd"]),
            orbit_number=int(context["orbit_number"]),
        )
        return job


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
    fg_types = db.query(LightcurveType).filter(
        ~bg_type_filter, LightcurveType.name != "QSPMagnitude"
    )

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
    aperture_map = dict(db.query(Aperture.id, Aperture.name))
    type_map = dict(db.query(LightcurveType.id, LightcurveType.name))
    orbit_map = dict(db.query(Orbit.id, Orbit.orbit_number))

    path_context = extract_pdo_path_context(path)
    constants_from_path = []

    if "orbit_number" in path_context:
        orbit_number = path_context.pop("orbit_number")
        constants_from_path.append(
            OrbitLightcurve.orbit_id == db.get_orbit_id(orbit_number)
        )
    columns = []
    for context in required_contexts:
        try:
            column = sa.sql.expression.literal_column(path_context[context])
            constants_from_path.append(
                getattr(OrbitLightcurve, context) == path_context[context]
            )
        except KeyError:
            column = getattr(OrbitLightcurve, context)
        columns.append(column)

    q = db.query(*columns).filter(*constants_from_path)
    logger.debug(f"Getting observations with {str(q)}")

    result = []
    for tic_id, camera, ccd, orbit_id, aperture_id, lightcurve_type_id in q:
        result.append(
            (
                tic_id,
                camera,
                ccd,
                orbit_map[orbit_id],
                aperture_map[aperture_id],
                type_map[lightcurve_type_id],
            )
        )
    return result


def look_for_relevant_files(wanted_tic_ids, lc_path):
    h5_files = lc_path.glob("*.h5")
    contexts = []
    n_accepted = 0
    for path in h5_files:
        context = extract_pdo_path_context(path)
        context["tic_id"] = int(context["tic_id"])
        context["camera"] = int(context["camera"])
        context["ccd"] = int(context["ccd"])
        context["orbit_number"] = int(context["orbit_number"])

        if context["tic_id"] in wanted_tic_ids:
            context["path"] = path
            contexts.append(context)
            n_accepted += 1

    logger.debug(f"Found {n_accepted} relevant files in {lc_path}")
    return contexts


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
                f"Created {len(naive_jobs)} jobs requiring dedup check "
                f"against {len(observed)} observed lightcurves"
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

    def fill_id_gaps(self):
        job_iter = iter(
            chain.from_iterable([j.orbit_lightcurve_jobs for j in self.jobs])
        )
        n_preassigned = 0
        logger.debug(
            "Preassigning orbit lightcurve ids with gaps in id sequence"
        )
        with self.db as db:
            try:
                for min_id, max_id in db.get_missing_id_ranges():
                    for id_ in range(min_id, max_id + 1):
                        job = next(job_iter)
                        job.preassigned_id = id_
                        n_preassigned += 1
            except StopIteration:
                # Out of jobs to assign ids to
                pass
        logger.debug(f"Preassigned {n_preassigned} ids")

    @property
    def tic_ids(self):
        return set(job.tic_id for job in self.jobs)


class TICListPlan(DirectoryPlan):
    def __init__(self, tic_ids, db):
        self.db = db
        self.tic_ids = set(tic_ids)
        self._look_for_files()
        self._preprocess_files()

    def _look_for_files(self):
        logger.debug(f"Looking for files relevant to {len(self.tic_ids)} tics")
        cameras = [1, 2, 3, 4]
        ccds = [1, 2, 3, 4]
        with self.db as db:
            orbits = [
                number
                for number, in db.query(Orbit.orbit_number).order_by(
                    Orbit.orbit_number
                )
            ]
        contexts = []
        orbit_dir = pathlib.Path("/pdo/qlp-data")
        for orbit_number, camera, ccd in product(orbits, cameras, ccds):
            orbit_path = orbit_dir / f"orbit-{orbit_number}" / "ffi"
            lc_path = orbit_path / f"cam{camera}/ccd{ccd}" / "LC"
            context = extract_pdo_path_context(lc_path)
            n_accepted = 0
            for path in lc_path.glob("*h5"):
                tic_id = int(path.name.split(".")[0])
                if tic_id in self.tic_ids:
                    context["tic_id"] = tic_id
                    context["path"] = path
                    contexts.append(context)
                    n_accepted += 1
            logger.debug(f"Found {n_accepted} relevant files in {lc_path}")
        self.files = contexts

    def _get_observed(self, db):
        q = (
            sa.select(
                OrbitLightcurve.tic_id,
                OrbitLightcurve.camera,
                OrbitLightcurve.ccd,
                Orbit.orbit_number,
                Aperture.name,
                LightcurveType.name,
            )
            .join(OrbitLightcurve.aperture)
            .join(OrbitLightcurve.lightcurve_type)
            .join(OrbitLightcurve.orbit)
            .where(OrbitLightcurve.tic_id.in_(self.tic_ids))
        )

        return set(db.execute(q).fetchall())


class FilePlan(DirectoryPlan):
    def __init__(self, plan_file_path, db):
        self.db = db
        self.plan_file_path = plan_file_path

        self._look_for_files()
        self._preprocess_files()

    def _parse_for_context(self, row):
        orbit_number, camera, ccd, tic_id = row
        base = pathlib.Path("/pdo/qlp-data")
        path = (
            base
            / f"orbit-{orbit_number}"
            / "ffi"
            / f"cam{camera}"
            / f"ccd{ccd}"
            / "LC"
            / f"{tic_id}.h5"
        )

        return {
            "tic_id": tic_id,
            "orbit_number": orbit_number,
            "camera": camera,
            "ccd": ccd,
            "path": path,
        }

    def _look_for_files(self):
        df = pd.read_csv(self.plan_file_path)[
            ["orbit_number", "camera", "ccd", "tic_id"]
        ]
        reduction = map(self._parse_for_context, df.itertuples(index=False))
        self.files = list(tqdm(reduction, total=len(df)))

    def _get_observed(self, db):
        unique_orbits = {c["orbit_number"] for c in self.files}
        q = (
            sa.select(
                OrbitLightcurve.tic_id,
                OrbitLightcurve.camera,
                OrbitLightcurve.ccd,
                Orbit.orbit_number,
                Aperture.name,
                LightcurveType.name,
            )
            .join(OrbitLightcurve.aperture)
            .join(OrbitLightcurve.lightcurve_type)
            .join(OrbitLightcurve.orbit)
            .where(Orbit.orbit_number.in_(unique_orbits))
        )
        return set(db.execute(q).fetchall())


class EM2Plan:
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
        self.get_observed_counts()
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
                files = source_dir.rglob("*.h5")
            else:
                files = source_dir.glob("*.h5")

            files = list(files)

            with Pool() as pool:
                results = pool.imap(extract_pdo_path_context, files)
                for path, context in zip(tqdm(files), results):
                    context["path"] = path
                    contexts.append(context)

            logger.debug(f"Found {len(files)} files in {source_dir}")

        self.contexts = contexts

    def get_observed_counts(self):
        q = (
            sa.select(
                ArrayOrbitLightcurve.tic_id,
                Orbit.orbit_number,
                sa.func.count(ArrayOrbitLightcurve.id),
            )
            .join(ArrayOrbitLightcurve.orbit)
            .where(ArrayOrbitLightcurve.tic_id.in_(self.tic_ids))
            .group_by(ArrayOrbitLightcurve.tic_id, Orbit.orbit_number)
        )
        observation_counts = {
            (tic_id, orbit): count
            for tic_id, orbit, count in self.db.execute(q)
        }

        self.observation_counts = observation_counts
        if len(observation_counts) > 0:
            counter = Counter(observation_counts.values())
            self.count_cutoff, most_common = counter.most_common(1)[0]
            logger.debug(
                f"Will ignore files that have {most_common} existing "
                "lightcurves"
            )
        else:
            logger.debug(
                "No existing observations found. Allowing all found files"
            )

    def _preprocess_files(self):
        logger.debug(f"Preprocessing {len(self.contexts)} files")
        jobs = []
        n_rejected = 0
        with Pool() as pool:
            results = pool.imap_unordered(
                EM2_H5_Job.from_path_context,
                tqdm(self.contexts, unit=" files"),
                chunksize=1000,
            )
            for job in results:
                n_lcs = self.observation_counts.get(
                    (job.tic_id, job.orbit_number), 0
                )
                if n_lcs < self.count_cutoff:
                    jobs.append(job)
                else:
                    n_rejected += 1

        logger.debug(
            f"Processed files and generated {len(jobs)} jobs,"
            f"ignoring {n_rejected} files."
        )
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

    @property
    def tic_ids(self):
        return set(job.tic_id for job in self.jobs)


class EM2_ArrayTICListPlan(EM2Plan):
    def __init__(self, tic_ids, db):
        self.db = db
        self._tic_ids = set(tic_ids)
        self._look_for_files()
        self.get_observed_counts()
        self._preprocess_files()

    @property
    def tic_ids(self):
        return self._tic_ids

    def _look_for_files(self):
        logger.debug(f"Looking for files relevant to {len(self.tic_ids)} tics")
        cameras = [1, 2, 3, 4]
        ccds = [1, 2, 3, 4]
        paths = []
        with self.db as db:
            orbits = [
                number
                for number, in db.query(Orbit.orbit_number).order_by(
                    Orbit.orbit_number
                )
            ]
        contexts = []
        orbit_dir = pathlib.Path("/pdo/qlp-data")
        for orbit_number, camera, ccd in product(orbits, cameras, ccds):
            orbit_path = orbit_dir / f"orbit-{orbit_number}" / "ffi"
            lc_path = orbit_path / f"cam{camera}" / f"ccd{ccd}" / "LC"
            paths.append(lc_path)

        with Pool() as pool:
            func = partial(look_for_relevant_files, self.tic_ids)
            contexts = list(
                chain.from_iterable(pool.imap_unordered(func, paths))
            )

        self.contexts = contexts
