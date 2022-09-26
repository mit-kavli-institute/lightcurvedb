from sqlalchemy import BigInteger, Column, ForeignKey, and_, select
from sqlalchemy.orm import relationship
from sqlalchemy.schema import UniqueConstraint

from lightcurvedb.core.base_model import CreatedOnMixin, QLPModel
from lightcurvedb.models.aperture import Aperture
from lightcurvedb.models.lightcurve import LightcurveType, OrbitLightcurve


class BestOrbitLightcurve(QLPModel, CreatedOnMixin):
    """
    A mapping of lightcurves to orbits to define the best detrending method
    used. This allows a heterogenous mix of lightcurves to coalese into a
    single timeseries.
    """

    __tablename__ = "best_orbit_lightcurves"
    __table_args = (
        UniqueConstraint(
            "tic_id",
            "orbit_id",
        ),
    )

    id = Column(BigInteger, primary_key=True)
    tic_id = Column(BigInteger, nullable=False)

    aperture_id = Column(ForeignKey("apertures.id", ondelete="RESTRICT"))
    lightcurve_type_id = Column(
        ForeignKey("lightcurvetypes.id", ondelete="RESTRICT")
    )
    orbit_id = Column(
        ForeignKey("orbits.id", ondelete="RESTRICT"), nullable=False
    )

    aperture = relationship("Aperture")
    lightcurve_type = relationship("LightcurveType")
    orbit = relationship("Orbit")

    @classmethod
    def orbitlightcurve_join_condition(cls):
        return and_(
            cls.tic_id == OrbitLightcurve.tic_id,
            cls.aperture_id == OrbitLightcurve.aperture_id,
            cls.lightcurve_type_id == OrbitLightcurve.lightcurve_type_id,
            cls.orbit_id == OrbitLightcurve.orbit_id,
        )


class BestOrbitLightcurveAPIMixin:
    def get_best_lightcurve_q(self):
        q = self.query(OrbitLightcurve).join(
            BestOrbitLightcurve.orbit_lightcurve,
        )
        return q

    def resolve_best_aperture_id(self, bestap):
        q = select(Aperture.id).filter(Aperture.name.ilike(f"%{bestap}%"))
        id_ = self.execute(q).fetchone()[0]
        return id_

    def resolve_best_lightcurve_type_id(self, detrend_name):
        q = select(LightcurveType.id).filter(
            LightcurveType.name.ilike(detrend_name.lower())
        )
        id_ = self.execute(q).fetchone()[0]
        return id_
