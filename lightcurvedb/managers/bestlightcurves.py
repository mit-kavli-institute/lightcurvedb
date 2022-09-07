import pandas as pd
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import aggregate_order_by

from lightcurvedb.managers.manager import BaseManager
from lightcurvedb.models import Lightpoint, OrbitLightcurve


def _agg_lightpoint_col(*cols):
    aggs = tuple(
        func.array_agg(aggregate_order_by(col, Lightpoint.cadence.asc()))
        for col in cols
    )
    return aggs


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
        super().__init__(db_config, template, Lightpoint.lightcurve_id)

    def load(self, tic_id):
        with self.db as db:
            q = (
                db.query(OrbitLightcurve.id)
                .join(OrbitLightcurve.best)
                .filter(OrbitLightcurve.tic_id == tic_id)
            )
            ids = [id_ for id_, in q]
            q = self.query_template.filter(self.identity_column.in_(ids))
            result = db.execute(q)
            self._cache[tic_id] = self.interpret_data(list(result))

    def interpret_data(self, result):
        arr = pd.DataFrame(
            result,
            columns=(
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
