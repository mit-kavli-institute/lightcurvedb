import multiprocessing as mp
from functools import partial

import cachetools
import numpy as np
import sqlalchemy as sa

from lightcurvedb import db_from_config, models
from lightcurvedb.models.lightpoint import LIGHTPOINT_NP_DTYPES
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


def resolve_ids(config, lightcurve_ids):
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
    q = (
        sa.select(
            models.Lightpoint.lightcurve_id,
            models.Lightpoint.cadence_array(),
            models.Lightpoint.barycentric_julian_date_array(),
            models.Lightpoint.data_array(),
            models.Lightpoint.error_array(),
            models.Lightpoint.x_centroid_array(),
            models.Lightpoint.y_centroid_array(),
            models.Lightpoint.quality_flag_array(),
        )
        .where(models.Lightpoint.lightcurve_id.in_(sorted(lightcurve_ids)))
        .group_by(models.Lightpoint.lightcurve_id)
    )

    results = {}
    with db_from_config(config) as db:
        for id_, *fields in db.execute(q).fetchall():
            results[id_] = {
                "cadence": fields[0],
                "barycentric_julian_date": fields[1],
                "data": fields[2],
                "error": fields[3],
                "x_centroid": fields[4],
                "y_centroid": fields[5],
                "quality_flag": fields[6],
            }

    return results


def resolve_lightcurve_ids(
    config, lightcurve_ids, workers=None, id_chunk_size=32
):
    """
    Resolve lightcurve ids using a multiprocessing queue. See ``resolve_ids``
    for behavior.

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
        lightcurve ids

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
    f = partial(resolve_ids, config)
    results = {}
    jobs = list(chunkify(sorted(lightcurve_ids), id_chunk_size))
    if workers is None:
        workers = min(mp.cpu_count(), len(jobs))
    with mp.Pool(workers) as pool:
        for result in pool.map(f, jobs):
            results.update(result)
    return results


class LightcurveManager:
    """
    This generic lightcurve manager allows interaction of lightcurve
    data with this general syntax:
    >>> lm = LightcurveManager(db_config)
    >>> lm[tic_id, aperture, type]["data"]
    """

    def __init__(self, config, cache_size=4096):
        self._config = config
        self._star_lookup = cachetools.LRUCache(cache_size)
        self._cache = cachetools.LRUCache(cache_size)

    def __getitem__(self, key):
        try:
            id_baseline = self._star_lookup[key]
        except KeyError:
            if isinstance(key, int):
                tic_id = key
                aperture = None
                type = None
            elif len(key) == 2:
                tic_id, aperture = key
                type = None
            elif len(key) == 3:
                tic_id, aperture, type = key
            else:
                raise IndexError(
                    "Could not interpret {key}. Indexing should be in one "
                    "of these forms: [tic_id], [tic_id, aperture], "
                    "[tic_id, aperture, lightcurve_type]"
                )
            self.lookup_ids_for_baseline(
                [tic_id], aperture=aperture, type=type
            )
            id_baseline = self._star_lookup[key]

        hot_ids = set(self._cache.keys())
        cache_misses = [id_ for id_ in id_baseline if id_ not in hot_ids]

        if len(cache_misses) > 0:
            for result in resolve_lightcurve_ids(self._config, cache_misses):
                for id_, data in result.items():
                    self._cache[id_] = data

        return self.construct_lightcurve(id_baseline)

    def lookup_ids_for_baseline(self, tic_ids, aperture=None, type=None):
        with db_from_config(self._cache) as db:
            q = (
                sa.select(
                    models.OrbitLightcurve.tic_id,
                    models.Aperture.name,
                    models.LightcurveType.name,
                    sa.func.array_agg(models.OrbitLightcurve.id),
                )
                .join(models.OrbitLightcurve.aperture)
                .join(models.OrbitLightcurve.lightcurve_type)
                .filter(models.OrbitLightcurve.tic_id.in_(tic_ids))
            )
            if aperture is not None:
                q = q.filter(models.Aperture.name == aperture)

            if type is not None:
                q = q.filter(models.LightcurveType.name == type)

            q = q.group_by(
                models.OrbitLightcurve.tic_id,
                models.Aperture.name,
                models.LightcurveType.name,
            )

            for *key, id_array in db.execute(q).fetchall():
                self._star_lookup[key] = id_array

    def yield_lightpoint_tuples_for(self, ids):
        for id_ in ids:
            data = self._cache[id_]
            yield from zip(*tuple(data[col] for col in LP_DATA_COLUMNS))

    def construct_lightcurve(self, ids):
        return np.array(
            list(self.yield_lightpoint_tuples_for(ids)),
            dtype=list(_np_dtype(*LP_DATA_COLUMNS)),
        )
