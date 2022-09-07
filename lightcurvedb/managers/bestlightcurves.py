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
        template = select(
            Lightpoint.cadence,
            Lightpoint.barycentric_julian_date,
            Lightpoint.data,
            Lightpoint.error,
            Lightpoint.x_centroid,
            Lightpoint.y_centroid,
            Lightpoint.quality_flag,
        ).order_by(Lightpoint.cadence)
        super().__init__(db_config, template, OrbitLightcurve.id)

    def load(self, tic_id):
        with self.db as db:
            q = db.query(OrbitLightcurve.id).filter_by(tic_id=tic_id)
            ids = [id_ for id_, in q]
            q = self.query_template.filter(self.identity_column.in_(ids))
            self._cache[tic_id] = self.interpret_data(list(q))

    def interpret_data(self, result):
        arr = np.array(
            result,
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
