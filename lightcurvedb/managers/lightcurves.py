import itertools as it
import multiprocessing as mp
from functools import partial

import cachetools
import numpy as np
import sqlalchemy as sa

from lightcurvedb import db_from_config, models
from lightcurvedb.core.tic8 import TIC8_DB
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

            dtype = tuple(_np_dtype(*LP_DATA_COLUMNS))
            struct = np.array(list(zip(*fields)), dtype=dtype)

            results[id_] = struct

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
    f = partial(resolve_ids, config)
    results = {}
    jobs = list(chunkify(sorted(lightcurve_ids), id_chunk_size))
    if workers is None:
        workers = min(mp.cpu_count(), len(jobs))
    if workers > 0:
        with mp.Pool(workers) as pool:
            for result in pool.map(f, jobs):
                results.update(result)
    else:
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

    def __init__(self, config, cache_size=4096, n_lc_readers=mp.cpu_count()):
        self._config = config
        self._keyword_lookups = {}
        self._id_to_tic_id_lookup = {}
        self._lightcurve_id_cache = cachetools.LRUCache(cache_size)
        self._lightpoint_cache = cachetools.LRUCache(cache_size)
        self._stellar_parameter_cache = cachetools.LRUCache(cache_size)
        self.n_lc_readers = n_lc_readers

    def __getitem__(self, key):
        tic_id = None
        apertures = []
        lightcurve_types = []

        if isinstance(key, int):
            # Just keying by integer
            tic_id = key
            raise NotImplementedError
        elif isinstance(key, tuple):
            # Tiered slice of data
            for token in key:
                if isinstance(token, int):
                    tic_id = token
                else:
                    if "Aperture" in token:
                        apertures.append(token)
                    else:
                        lightcurve_types.append(token)
        else:
            raise KeyError(f"Cannot access lightcurve data using {key}.")

        return self.get_lightcurve(
            tic_id, apertures=apertures, lightcurve_types=lightcurve_types
        )

    def get_lightcurve(self, tic_id, apertures=None, lightcurve_types=None):
        """ """
        if apertures is None or len(apertures) == 0:
            # Pivot around types
            apertures = [
                keywords[0] for keywords in self._keyword_lookup[tic_id]
            ]
        if lightcurve_types is None or len(lightcurve_types) == 0:
            lightcurve_types = [
                keywords[1] for keywords in self._keyword_lookup[tic_id]
            ]
        keys = list(it.product([tic_id], apertures, lightcurve_types))
        if len(keys) == 1:
            raise NotImplementedError

        result = {}
        if len(apertures) == 1:
            for aperture, type in keys:
                result[type] = self.construct_lightcurve(
                    self._resolve_key(tic_id, aperture, type)
                )
        elif len(lightcurve_types) == 1:
            for aperture, type in keys:
                result[aperture] = self.construct_lightcurve(
                    self._resolve_key(tic_id, aperture, type)
                )

        else:
            # Composite keys...construct tiered dictionary
            for aperture, type in keys:
                types = result.get(aperture, dict())
                types[type] = self.construct_lightcurve(
                    self._resolve_key(tic_id, aperture, type)
                )
                result[aperture] = types

        return result

    def get_magnitude_median_offset(self, id, struct):
        tic_id = self._id_to_tic_id_lookup[id]
        try:
            tmag = self._stellar_parameter_cache[tic_id]
        except KeyError:
            with TIC8_DB() as db:
                q = sa.select(db.ticentries.c.tmag).where(
                    db.ticentries.id == tic_id
                )
                tmag = db.execute(q).fetchone()[0]

            self._stellar_parameter_cache[tic_id] = tmag

        good_cadences = struct["quality_flag"] == 0
        mag_median = np.nanmedian(struct["data"][good_cadences])
        offset = mag_median - tmag

        return offset

    def _resolve_lightcurve_ids_for(self, tic_id, aperture, lightcurve_type):
        q = (
            sa.select(
                models.OrbitLightcurve.tic_id,
                models.Aperture.name,
                models.LightcurveType.name,
                sa.func.array_agg(models.OrbitLightcurve.id),
            )
            .join(models.OrbitLightcurve.aperture)
            .join(models.OrbitLightcurve.lightcurve_type)
            .where(models.OrbitLightcurve.tic_id == tic_id)
            .group_by(
                models.OrbitLightcurve.tic_id,
                models.Aperture.name,
                models.LightcurveType.name,
            )
        )

        with db_from_config(self._config) as db:
            for tic_id, aperture, type, ids in db.execute(q):
                yield (tic_id, aperture, type), ids

    def _resolve_key(self, tic_id, aperture, lightcurve_type):
        idx = (tic_id, aperture, lightcurve_type)
        try:
            ids = self._lightcurve_id_cache[idx]
        except KeyError:
            ids = self._resolve_lightcurve_ids_for(*idx)

            for id in ids:
                self._id_to_tic_id_lookup[id] = tic_id

            self._lightcurve_id_cache[idx] = ids
        return ids

    def construct_lightcurve(self, ids):
        datum = []
        misses = []
        for id in ids:
            try:
                data = self._lightpoint_cache[id]
                datum.append(data)
            except KeyError:
                misses.append(id)

        if len(misses) > 0:
            search = resolve_ids(
                self._config, misses, workers=self.n_lc_readers
            )
            for id, data in search:
                offset = self.get_magnitude_median_offset(id, data)
                data["data"] -= offset
                self._lightpoint_cache[id] = data
                datum.append(data)

        full_struct = np.concatenate(
            sorted(datum, key=lambda struct: struct["cadence"][0])
        )
        return full_struct
