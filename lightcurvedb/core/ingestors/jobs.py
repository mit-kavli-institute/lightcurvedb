import pathlib
from collections import Counter
from dataclasses import dataclass
from functools import partial
from itertools import chain, product
from multiprocessing import Pool, cpu_count

import sqlalchemy as sa
from loguru import logger
from tqdm import tqdm

from lightcurvedb import db_from_config
from lightcurvedb.models import ArrayOrbitLightcurve, Orbit
from lightcurvedb.util.contexts import extract_pdo_path_context


@dataclass
class H5_Job:
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


def look_for_relevant_files(config, lc_path, tic_mask=None):
    try:
        h5_files = lc_path.glob("*.h5")
        contexts = []
        n_accepted = 0
        n_rejected = 0
        path_context = extract_pdo_path_context(lc_path)
        lc_histogram_q = (
            sa.select(
                ArrayOrbitLightcurve.tic_id,
                sa.func.count(ArrayOrbitLightcurve.tic_id).label("lc_count"),
            )
            .join(ArrayOrbitLightcurve.orbit)
            .filter(
                Orbit.orbit_number == path_context["orbit_number"],
                ArrayOrbitLightcurve.camera == path_context["camera"],
                ArrayOrbitLightcurve.ccd == path_context["ccd"],
            )
            .group_by(ArrayOrbitLightcurve.tic_id)
        )
        with db_from_config(config) as db:
            logger.debug(f"Querying for existing observations for {lc_path}")
            observation_counts = {
                tic_id: lc_count
                for tic_id, lc_count in db.execute(lc_histogram_q)
            }
            if len(observation_counts) > 0:
                counter = Counter(observation_counts.values())
                count_cutoff, _ = counter.most_common(1)[0]
                logger.debug(
                    f"Will ignore files that have >= {count_cutoff} "
                    "observations for orbit "
                    "{orbit_number} camera {camera} ccd {ccd}".format(
                        **path_context
                    )
                )
            else:
                count_cutoff = None
    except KeyError:
        logger.warning(
            "Could not determine good orbit, camera, ccd contexts for "
            f"{lc_path}"
        )
        return []

    for path in h5_files:
        context = extract_pdo_path_context(path)
        context["tic_id"] = int(context["tic_id"])
        context["camera"] = int(context["camera"])
        context["ccd"] = int(context["ccd"])
        context["orbit_number"] = int(context["orbit_number"])

        observed_n_times = observation_counts.get(context["tic_id"], 0)
        if tic_mask is None:
            in_mask = True
        else:
            in_mask = context["tic_id"] in tic_mask

        above_cutoff = (
            count_cutoff is not None and observed_n_times >= count_cutoff
        )

        if not in_mask or above_cutoff:
            n_rejected += 1
            continue

        context["path"] = path
        contexts.append(context)
        n_accepted += 1

    logger.debug(
        f"Found {n_accepted} relevant files in {lc_path}, "
        f"rejecting {n_rejected} files"
    )
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
        db_config,
        recursive=False,
    ):
        self.source_dirs = directories
        self.recursive = recursive
        self.db_config = db_config
        self._look_for_files()
        self._preprocess_files()

    def __repr__(self):
        file_msg = f"Considered {len(self.files)} files"
        tics = {context.tic_id for context in self.jobs}
        tic_msg = f"{len(tics)} total unique TIC ids"

        messages = [file_msg, tic_msg]

        return "\n".join(messages)

    def _look_for_files(self):
        n_workers = min((len(self.source_dirs), cpu_count()))
        func = partial(look_for_relevant_files, self.db_config)
        with Pool(n_workers) as pool:
            results = pool.imap(func, self.source_dirs)
            contexts = list(chain.from_iterable(results))

        self._tic_ids = {int(c["tic_id"]) for c in contexts}
        self.contexts = contexts

    def _preprocess_files(self):
        logger.debug(f"Preprocessing {len(self.contexts)} files")
        jobs = []
        with Pool() as pool:
            results = pool.imap_unordered(
                H5_Job.from_path_context,
                tqdm(self.contexts, unit=" files"),
                chunksize=1000,
            )
            jobs.extend(results)

        logger.debug(f"Processed files and generated {len(jobs)} jobs")
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
        return self._tic_ids


class TICListPlan(DirectoryPlan):
    def __init__(self, tic_ids, db_config):
        self.db_config = db_config
        self._tic_ids = set(tic_ids)
        self._look_for_files()
        self._preprocess_files()

    def _look_for_files(self):
        logger.debug(f"Looking for files relevant to {len(self.tic_ids)} tics")
        cameras = [1, 2, 3, 4]
        ccds = [1, 2, 3, 4]
        paths = []
        with db_from_config(self.db_config) as db:
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
            func = partial(
                look_for_relevant_files, self.db_config, tic_mask=self.tic_ids
            )
            contexts = list(
                chain.from_iterable(pool.imap_unordered(func, paths))
            )

        self.contexts = contexts
