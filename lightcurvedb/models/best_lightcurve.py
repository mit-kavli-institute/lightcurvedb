from lightcurvedb.core.base_model import QLPReference
from lightcurvedb.models.frame import Frame
from lightcurvedb.models.lightpoint import Lightpoint
from sqlalchemy import Column, ForeignKey, select, func, and_, BigInteger
from sqlalchemy.orm import relationship
from sqlalchemy.ext.hybrid import hybrid_property, hybrid_method
from sqlalchemy.schema import UniqueConstraint


class BestOrbitLightcurve(QLPReference):
    """
    A mapping of lightcurves to orbits to define the best detrending method
    used. This allows a heterogenous mix of lightcurves to coalese into a
    single timeseries.
    """

    __tablename__ = "best_orbit_lightcurves"
    __table_args = (
        UniqueConstraint(
            "lightcurve_id",
            "orbit_id",
        ),
    )

    id = Column(BigInteger, primary_key=True)
    lightcurve_id = Column(
        ForeignKey("lightcurves.id", ondelete="CASCADE"), nullable=False
    )
    orbit_id = Column(
        ForeignKey("orbits.id", ondelete="RESTRICT"), nullable=False
    )

    lightcurve = relationship("Lightcurve")

    orbit = relationship("Orbit")

    @hybrid_method
    def max_cadence(self, frame_type="Raw FFI"):
        return self.orbit.max_cadence()

    @max_cadence.expression
    def max_cadence(cls, frame_type="Raw FFI"):
        return (
            select(func.max(Frame.cadence))
            .where(Frame.orbit_id == cls.orbit_id)
            .label("max_cadence")
        )

    @hybrid_method
    def min_cadence(self, frame_type="Raw FFI"):
        return self.orbit.min_cadence()

    @min_cadence.expression
    def min_cadence(cls, frame_type="Raw FFI"):
        return (
            select(func.min(Frame.cadence))
            .where(Frame.orbit_id == cls.orbit_id)
            .label("min_cadence")
        )

    @hybrid_method
    def lightpoints(self, frame_type="Raw FFI"):
        raise NotImplementedError

    @lightpoints.expression
    def lightpoints(cls, frame_type="Raw FFI"):
        return (
            select(
                Lightpoint.lightcurve_id,
                Lightpoint.cadence,
                Lightpoint.barycentric_julian_date,
                Lightpoint.data,
                Lightpoint.error,
                Lightpoint.x_centroid,
                Lightpoint.y_centroid,
                Lightpoint.quality_flag,
            )
            .where(
                and_(
                    Lightpoint.lightcurve_id == cls.lightcurve_id,
                    Lightpoint.cadence.between(
                        cls.min_cadence, cls.max_cadence
                    ),
                )
            )
            .label("lightpoints")
        )
