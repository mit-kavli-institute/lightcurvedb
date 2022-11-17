import sqlalchemy as sa
from sqlalchemy import BigInteger, Column, ForeignKey, and_, select
from sqlalchemy.orm import relationship
from sqlalchemy.schema import UniqueConstraint

from lightcurvedb.core.base_model import CreatedOnMixin, QLPModel
from lightcurvedb.models.aperture import Aperture
from lightcurvedb.models.lightcurve import ArrayOrbitLightcurve, LightcurveType


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

    aperture_id = Column(
        ForeignKey("apertures.id", ondelete="RESTRICT", index=True)
    )
    lightcurve_type_id = Column(
        ForeignKey("lightcurvetypes.id", ondelete="RESTRICT"),
        index=True,
    )
    orbit_id = Column(
        ForeignKey("orbits.id", ondelete="RESTRICT"), nullable=False
    )

    aperture = relationship("Aperture")
    lightcurve_type = relationship("LightcurveType")
    orbit = relationship("Orbit")

    @classmethod
    def lightcurve_join(cls, OtherLightcurve):
        return and_(
            cls.tic_id == OtherLightcurve.tic_id,
            cls.aperture_id == OtherLightcurve.aperture_id,
            cls.lightcurve_type_id == OtherLightcurve.lightcurve_type_id,
            cls.orbit_id == OtherLightcurve.orbit_id,
        )


class BestOrbitLightcurveAPIMixin:
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

    def get_best_lightcurve_baseline(
        self, tic_id, aperture=None, lightcurve_type=None
    ):
        BEST_LC = BestOrbitLightcurve
        LC = ArrayOrbitLightcurve

        q = sa.select(ArrayOrbitLightcurve)
        join_conditions = []
        filter_conditions = [LC.tic_id == tic_id]

        if aperture is None:
            join_conditions.append(BEST_LC.aperture_id == LC.aperture_id)
        else:
            q = q.join(LC.aperture)
            filter_conditions.append(Aperture.name == aperture)
        if lightcurve_type is None:
            join_conditions.append(
                BEST_LC.lightcurve_type_id == LC.lightcurve_type_id
            )
        else:
            q = q.join(LC.lightcurve_type)
            filter_conditions.append(LightcurveType.name == lightcurve_type)

        if len(join_conditions) > 0:
            join_conditions.append(BEST_LC.tic_id == LC.tic_id)
            join_conditions.append(BEST_LC.orbit_id == LC.orbit_id)
            q = q.join(BEST_LC, sa.and_(*join_conditions))
        q = q.where(*filter_conditions)

        return self._process_lc_selection(q)
