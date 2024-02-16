"""
This module describes the best-orbit lightcurve manager subclasses
"""
from itertools import groupby

import cachetools
import numpy as np
import sqlalchemy as sa

from lightcurvedb import db_from_config
from lightcurvedb import models as m
from lightcurvedb.managers.lightcurves import (
    LightcurveManager,
    _nested_defaultdict,
)
from lightcurvedb.util.constants import DEFAULT_CONFIG_PATH


class BestLightcurveManager(LightcurveManager):
    """
    A class for managing "best" lightcurves. This class is accessed by
    tic ids.

    Example
    -------
    ```python

    mgr = BestLightcurveManager(db_config, normalize=True)
    lc = mgr[38696575]  # Emits query

    lc["cadence"] # np.array([1, 2, 3, 4])
    lc["data"] # np.array([0.0, 10.0, 4.0, nan])
    lc["quality_flag"] # np.array([0, 0, 0, 1])
    ```
    """

    def __init__(
        self,
        config=None,
        cache_size=4096,
        aperture_constraint=None,
        type_constraint=None,
    ):
        self._config = DEFAULT_CONFIG_PATH if config is None else config
        self._stellar_parameter_cache = cachetools.LRUCache(cache_size)
        self.aperture_constraint = aperture_constraint
        self.type_constraint = type_constraint

    def __getitem__(self, key):
        return self.get_lightcurve(key)

    def _execute_q(self, q):
        result = _nested_defaultdict()

        with db_from_config(self._config) as db:
            groups = groupby(db.execute(q), lambda row: row[0])
            for tic_id, group in groups:
                data = [row[1:] for row in group]
                result[tic_id] = self.construct_lightcurve(tic_id, data)

        return self._reduce_defaultdict(result)

    def get_lightcurve(self, tic_ids):
        q = (
            sa.select(
                m.ArrayOrbitLightcurve.tic_id,
                m.ArrayOrbitLightcurve.cadences,
                m.ArrayOrbitLightcurve.barycentric_julian_dates,
                m.ArrayOrbitLightcurve.data,
                m.ArrayOrbitLightcurve.errors,
                m.ArrayOrbitLightcurve.x_centroids,
                m.ArrayOrbitLightcurve.y_centroids,
                m.ArrayOrbitLightcurve.quality_flags,
            )
            .join(m.ArrayOrbitLightcurve.orbit)
            .order_by(
                m.ArrayOrbitLightcurve.tic_id.asc(), m.Orbit.orbit_number.asc()
            )
        )

        join_conditions = [
            m.BestOrbitLightcurve.tic_id == m.ArrayOrbitLightcurve.tic_id,
            m.BestOrbitLightcurve.orbit_id == m.ArrayOrbitLightcurve.orbit_id,
        ]

        if self.aperture_constraint is None:
            join_conditions.append(
                m.BestOrbitLightcurve.aperture_id
                == m.ArrayOrbitLightcurve.aperture_id
            )
        else:
            q = q.join(m.ArrayOrbitLightcurve.aperture)
            q = q.filter(m.Aperture.name == self.aperture_constraint)

        if self.type_constraint is None:
            join_conditions.append(
                m.BestOrbitLightcurve.lightcurve_type_id
                == m.ArrayOrbitLightcurve.lightcurve_type_id
            )
        else:
            q = q.join(m.ArrayOrbitLightcurve.lightcurve_type)
            q = q.filter(m.LightcurveType.name == self.type_constraint)

        q = q.join(m.BestOrbitLightcurve, sa.and_(*join_conditions))

        if isinstance(tic_ids, (int, np.integer)):
            q = q.where(m.ArrayOrbitLightcurve.tic_id == tic_ids)
        elif len(tic_ids) == 1:
            q = q.where(m.ArrayOrbitLightcurve.tic_id == list(tic_ids)[0])
        else:
            q = q.where(m.ArrayOrbitLightcurve.tic_id.in_(tic_ids))

        return self._execute_q(q)
