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
