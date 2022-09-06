import numpy as np
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import aggregate_order_by

from lightcurvedb.managers.manager import BaseManager
from lightcurvedb.models import Lightpoint, OrbitLightcurve
from lightcurvedb.models.lightpoint import LIGHTPOINT_NP_DTYPES


def _agg_lightpoint_col(*cols):
    aggs = tuple(
        func.array_agg(aggregate_order_by(col, Lightpoint.cadence.asc()))
        for col in cols
    )
    return aggs


def _make_dtype(*names):
    dtype = []
    for name in names:
        dtype.append((name, LIGHTPOINT_NP_DTYPES[name]))
    return dtype


class BestLightcurveManager(BaseManager):
    def __init__(self, db_config):
        template = (
            select(
                OrbitLightcurve.id,
                *_agg_lightpoint_col(
                    Lightpoint.cadence,
                    Lightpoint.barycentric_julian_date,
                    Lightpoint.data,
                    Lightpoint.error,
                    Lightpoint.x_centroid,
                    Lightpoint.y_centroid,
                    Lightpoint.quality_flag,
                )
            )
            .join(
                OrbitLightcurve, OrbitLightcurve.id == Lightpoint.lightcurve_id
            )
            .group_by(OrbitLightcurve.id)
        )
        super().__init__(db_config, template, OrbitLightcurve.id)

    def interpret_data(self, data_aggregate):
        _iter = zip(*data_aggregate)
        arr = np.array(
            _iter,
            dtype=_make_dtype(
                "cadence",
                "barycentric_julian_date",
                "data",
                "error",
                "x_centroid",
                "y_centroid",
                "quality_flag",
            ),
        )
        return arr
