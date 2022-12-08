import multiprocessing as mp
from collections import defaultdict
from itertools import groupby

import cachetools
import numpy as np
import pyticdb
import sqlalchemy as sa

from lightcurvedb import db_from_config
from lightcurvedb import models as m
from lightcurvedb.util.constants import DEFAULT_CONFIG_PATH

LP_DATA_COLUMNS = (
    "cadence",
    "barycentric_julian_date",
    "data",
    "error",
    "x_centroid",
    "y_centroid",
    "quality_flag",
)


def _nested_defaultdict():
    """
    A quick infinitely nestable default dictionary.
    """
    return defaultdict(_nested_defaultdict)


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
        self._config = DEFAULT_CONFIG_PATH if config is None else config
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
            groups = groupby(
                db.execute(q), lambda row: (row[0], row[1], row[2])
            )
            for (tic_id, aperture, type), group in groups:
                data = [row[3:] for row in group]
                result[tic_id][aperture][type] = self.construct_lightcurve(
                    tic_id, data
                )

        return self._reduce_defaultdict(result)

    def get_lightcurve(self, tic_ids, apertures=None, lightcurve_types=None):
        """ """
        q = (
            sa.select(
                m.ArrayOrbitLightcurve.tic_id,
                m.LightcurveType.name,
                m.Aperture.name,
                m.ArrayOrbitLightcurve.cadences,
                m.ArrayOrbitLightcurve.barycentric_julian_dates,
                m.ArrayOrbitLightcurve.data,
                m.ArrayOrbitLightcurve.errors,
                m.ArrayOrbitLightcurve.x_centroids,
                m.ArrayOrbitLightcurve.y_centroids,
                m.ArrayOrbitLightcurve.quality_flags,
            )
            .join(m.ArrayOrbitLightcurve.aperture)
            .join(m.ArrayOrbitLightcurve.lightcurve_type)
            .join(m.ArrayOrbitLightcurve.orbit)
            .where(m.ArrayOrbitLightcurve.tic_id.in_(tic_ids))
            .order_by(
                m.ArrayOrbitLightcurve.tic_id.asc(),
                m.LightcurveType.id.asc(),
                m.Aperture.id.asc(),
                m.Orbit.orbit_number.asc(),
            )
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
            "barycentric_julian_dates",
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
