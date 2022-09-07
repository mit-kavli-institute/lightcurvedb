"""
This module describes the best-orbit lightcurve manager subclasses
"""

import numpy as np
from sqlalchemy import and_, func, select
from sqlalchemy.dialects.postgresql import aggregate_order_by

from lightcurvedb.core.tic8 import one_off
from lightcurvedb.managers.manager import BaseManager
from lightcurvedb.models import (
    BestOrbitLightcurve,
    Lightpoint,
    OrbitLightcurve,
)
from lightcurvedb.models.lightpoint import LIGHTPOINT_NP_DTYPES


def _np_dtype(*cols):
    for col in cols:
        yield col, LIGHTPOINT_NP_DTYPES[col]


def _agg_lightpoint_col(*cols):
    aggs = tuple(
        func.array_agg(aggregate_order_by(col, Lightpoint.cadence.asc()))
        for col in cols
    )
    return aggs


class BestLightcurveManager(BaseManager):
    def __init__(self, db_config, normalize=True):
        """
        Initialize a best-lightcurve manager.

        Parameters
        ----------
        db_config: pathlike
            A path to an lcdb configuration file
        normalize: bool, optional
            If true, normalize returned lightcurves to their
            corresponding tmag values.
        """
        template = select(
            Lightpoint.lightcurve_id,
            *_agg_lightpoint_col(
                Lightpoint.cadence,
                Lightpoint.barycentric_julian_date,
                Lightpoint.data,
                Lightpoint.error,
                Lightpoint.x_centroid,
                Lightpoint.y_centroid,
                Lightpoint.quality_flag,
            )
        ).group_by(Lightpoint.lightcurve_id)
        self.normalize = normalize
        super().__init__(db_config, template, Lightpoint.lightcurve_id)

    def load(self, tic_id):
        if self.normalize:
            tmag = one_off(tic_id, "tmag")

        with self.db as db:
            q = (
                db.query(OrbitLightcurve.id)
                .join(
                    BestOrbitLightcurve,
                    and_(
                        BestOrbitLightcurve.orbit_id
                        == OrbitLightcurve.orbit_id,
                        BestOrbitLightcurve.aperture_id
                        == OrbitLightcurve.aperture_id,
                        BestOrbitLightcurve.lightcurve_type_id
                        == OrbitLightcurve.lightcurve_type_id,
                        BestOrbitLightcurve.tic_id == OrbitLightcurve.tic_id,
                    ),
                )
                .filter(OrbitLightcurve.tic_id == tic_id)
            )
            ids = [id_ for id_, in q]
            q = self.query_template.filter(self.identity_column.in_(ids))
            result = db.execute(q)
            lps = []
            for id_, *data in result:
                lp = self.interpret_data(data)
                if self.normalize:
                    mask = lp["quality_flag"] == 0
                    median = np.nanmedian(lp[mask]["data"])
                    offset = median - tmag
                    lp["data"] -= offset
                lps.append(lp)
            self._cache[tic_id] = np.concatenate(lps)

    def interpret_data(self, result):
        arr = np.array(
            list(zip(*result)),
            dtype=list(
                _np_dtype(
                    "cadence",
                    "barycentric_julian_date",
                    "data",
                    "error",
                    "x_centroid",
                    "y_centroid",
                    "quality_flag",
                )
            ),
        )
        return arr
