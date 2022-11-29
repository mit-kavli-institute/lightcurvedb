import multiprocessing as mp
from collections import defaultdict
from functools import partial

import cachetools
import numpy as np
import pyticdb
import sqlalchemy as sa

from lightcurvedb import db_from_config
from lightcurvedb import models as m
from lightcurvedb.models.lightpoint import LIGHTPOINT_NP_DTYPES
from lightcurvedb.util.constants import __DEFAULT_PATH__
from lightcurvedb.util.iter import chunkify

LP_DATA_COLUMNS = (
    "cadence",
    "barycentric_julian_date",
    "data",
    "error",
    "x_centroid",
    "y_centroid",
    "quality_flag",
)


def _np_dtype(*cols):
    for col in cols:
        yield col, LIGHTPOINT_NP_DTYPES[col]


def _nested_defaultdict():
    """
    A quick infinitely nestable default dictionary.
    """
    return defaultdict(_nested_defaultdict)


def fetch_lightcurve_data(config, lightcurve_ids):
    """
    Resolve lightpoint data from the given lightcurve ids.

    Parameters
    ----------
    config: pathlike
        Path to a configuration file for a lightcurvedb db instance.

    lightcurve_ids: List[int]
        A list of lightcurve ids to get orbit baselines for.

    Returns
    -------
    Dict[int, Dict[int, List]]
        A dictionary of lightcurve ids -> data where data is also a
        dictionary of baseline-name -> array.

    Note
    ----
    Duplicates in the input id list will result in overwrites in the return
    dictionary. Order is also not guaranteed in the return.
    """
    q = sa.select(m.ArrayOrbitLightcurve).where(
        m.ArrayOrbitLightcurve.id.in_(lightcurve_ids)
    )
    results = {}
    with db_from_config(config) as db:
        for lightcurve in db.execute(q).scalars().all():
            results[lightcurve.id] = lightcurve

    return results


def fetch_lightcurve_data_multiprocessing(
    config, lightcurve_ids, workers=None, id_chunk_size=32
):
    """
    Resolve lightcurve ids using a multiprocessing queue. See
    ``fetch_lightcurve_data`` for behavior.

    Parameters
    ----------
    config: pathlike
        Path to a configuration file for a lightcurvedb db instance.

    lightcurve_ids: List[int]
        A list of lightcurve ids to get orbit baselines for.

    workers: int, optional
        The `maximum` number of multiprocess workers to use. By default
        this is ``mp.cpu_count()``. This function will use the minimum of
        either the specified cpu count or the length of the chunkified
        lightcurve ids.

        If this value is set to 0, then no multiprocessing workers will be
        spawned; instead, fetching logic will be done in the current process.

    id_chunk_size: int, optional
        Granularity of the multiprocess workload. It's inefficient
        to query a lightcurve at a time for both remote resource utilization
        and python serialization of the return (which could be in the order
        of millions to billions of rows).

    Returns
    -------
    Dict[int, Dict[int, List]]
        A dictionary of lightcurve ids -> data where data is also a
        dictionary of baseline-name -> array.

    Note
    ----
    Duplicates in the input id list will result in overwrites in the return
    dictionary. Order is also not guaranteed in the return.

    As of the latest doc, it is unknown of the best chunk size to use.
    """
    f = partial(fetch_lightcurve_data, config)
    results = {}
    jobs = list(chunkify(sorted(lightcurve_ids), id_chunk_size))
    if workers is None:
        workers = min(mp.cpu_count(), len(jobs))
    if workers > 0:
        with mp.Pool(workers) as pool:
            for result in pool.map(f, jobs):
                results.update(result)
    else:
        for chunk in jobs:
            for result in f(chunk):
                results.update(result)

    return results


def resolve_keywords_for(config, tic_id):
    q = (
        sa.select(m.Aperture.name, m.LightcurveType.name)
        .join(m.ArrayOrbitLightcurve.aperture)
        .join(m.ArrayOrbitLightcurve.lightcurve_type)
        .filter(m.ArrayOrbitLightcurve.tic_id == tic_id)
        .distinct()
    )
    with db_from_config(config) as db:
        return db.execute(q).fetchall()


class LightcurveManager:
    """
    This generic lightcurve manager allows interaction of lightcurve
    data with this general syntax:
    >>> lm = LightcurveManager(db_config)
    >>> lm[tic_id, aperture, type]["data"]
    """

    def __init__(
        self, config=None, cache_size=4096, n_lc_readers=mp.cpu_count()
    ):
        self._config = __DEFAULT_PATH__ if config is None else config
        self._stellar_parameter_cache = cachetools.LRUCache(cache_size)
        self.n_lc_readers = n_lc_readers

    def __getitem__(self, key):
        tic_ids = []
        apertures = []
        lightcurve_types = []

        if isinstance(key, int):
            # Just keying by integer
            tic_ids.append(key)
        elif isinstance(key, tuple):
            # Tiered slice of data
            for token in key:
                if isinstance(token, int):
                    tic_id = token
                    tic_ids.append(tic_id)
                else:
                    if "Aperture" in token:
                        apertures.append(token)
                    else:
                        lightcurve_types.append(token)
        else:
            raise KeyError(f"Cannot access lightcurve data using {key}.")

        if len(tic_ids) == 0:
            raise KeyError("You must specify at least 1 TIC id.")

        return self.get_lightcurve(
            tic_ids, apertures=apertures, lightcurve_types=lightcurve_types
        )

    @classmethod
    def _reduce_defaultdict(cls, dictionary):
        """
        Quickly flatten dictionaries if their keys are only of length 1.
        """
        if len(dictionary) == 1:
            key = list(dictionary.keys())[0]
            return dictionary[key]
        else:
            keys = list(dictionary.keys())
            for key in keys:
                if isinstance(dictionary[key], dict):
                    dictionary[key] = cls._reduce_defaultdict(dictionary[key])
        return dictionary

    def _execute_q(self, q):
        result = _nested_defaultdict()

        with db_from_config(self._config) as db:
            for tic_id, aperture, type, *data in db.execute(q):
                result[tic_id][aperture][type] = self.construct_lightcurve(
                    tic_id, data
                )

        return self._reduce_defaultdict(result)

    def keywords_for(self, tic_id):
        try:
            keywords = self._keyword_lookups[tic_id]
        except KeyError:
            keywords = resolve_keywords_for(self._config, tic_id)
            self._keyword_lookups[tic_id] = keywords
        return keywords

    def get_lightcurve(self, tic_ids, apertures=None, lightcurve_types=None):
        """ """
        q = (
            sa.select(
                m.ArrayOrbitLightcurve.tic_id,
                m.LightcurveType.name,
                m.Aperture.name,
                sa.func.array_agg(m.ArrayOrbitLightcurve.cadences),
                sa.func.array_agg(
                    m.ArrayOrbitLightcurve.barycentric_julian_dates
                ),
                sa.func.array_agg(m.ArrayOrbitLightcurve.data),
                sa.func.array_agg(m.ArrayOrbitLightcurve.errors),
                sa.func.array_agg(m.ArrayOrbitLightcurve.x_centroids),
                sa.func.array_agg(m.ArrayOrbitLightcurve.y_centroids),
                sa.func.array_agg(m.ArrayOrbitLightcurve.quality_flags),
            )
            .join(m.ArrayOrbitLightcurve.aperture)
            .join(m.ArrayOrbitLightcurve.lightcurve_type)
            .where(m.ArrayOrbitLightcurve.tic_id.in_(tic_ids))
        )

        if apertures is not None:
            if len(apertures) == 1:
                q = q.where(m.Aperture.name == apertures[0])
            else:
                q = q.where(m.Aperture.name.in_(apertures))
        if lightcurve_types is not None:
            if len(lightcurve_types) == 1:
                q = q.where(m.LightcurveType.name == lightcurve_types[0])
            else:
                q = q.where(m.Lightcurvetype.name.in_(lightcurve_types))

        return self._execute_q(q)

    def get_magnitude_median_offset(self, tic_id, struct):
        try:
            tmag = self._stellar_parameter_cache[tic_id]
        except KeyError:
            tmag = pyticdb.query_by_id(tic_id, "tmag")[0]

            self._stellar_parameter_cache[tic_id] = tmag

        good_cadences = struct["quality_flags"] == 0
        mag_median = np.nanmedian(struct["data"][good_cadences])
        offset = mag_median - tmag

        return offset

    def construct_lightcurve(self, tic_id, data_list):
        datum = []
        columns = (
            "cadences",
            "barycentric_julian_date",
            "data",
            "errors",
            "x_centroids",
            "y_centroids",
            "quality_flags",
        )
        dtype = m.ArrayOrbitLightcurve.create_structured_dtype(*columns)
        for row_aggregate in data_list:
            struct = np.array(list(zip(*row_aggregate)), dtype=dtype)
            offset = self.get_magnitude_median_offset(tic_id, struct)
            struct["data"] -= offset
            datum.append(struct)

        full_struct = np.concatenate(
            sorted(datum, key=lambda struct: struct["cadences"][0])
        )
        return full_struct
