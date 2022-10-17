from sqlalchemy import BigInteger, Column, ForeignKey, and_, select
import sqlalchemy as sa
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

    def get_best_lightcurve_baseline(self,
        tic_id,
        lightcurve_type=None,
        aperture=None
        ):
        """
        Find the best lightcurve for the given TIC id.
        By default, the best aperture and best detrending
        methods are joined with the lightcurve table to
        provide a potentially mixed lightcurve.

        You may override which type or apertures are
        used in the join. If both aperture and types
        are specified then this method is equivalent to
        ``db.get_lightcurve_baseline``.
        """
        columns = [
            "cadence",
            "barycentric_julian_date",
            "data",
            "error",
            "x_centroid",
            "y_centroid",
            "quality_flag",
        ]
        best_lc = BestOrbitLightcurve
        lc = OrbitLightcurve
        id_q = sa.select(lc.id)
        join_conditions = [
            best_lc.tic_id == lc.tic_id,
            best_lc.orbit_id == lc.orbit_id,
        ]
        filter_conditions = [
            lc.tic_id == tic_id
        ]

        if aperture is None:
            join_conditions.append(
                best_lc.aperture_id == lc.aperture_id
            )
        else:
            id_q = id_q.join(
                lc.aperture
            )
            filter_conditions.append(
                Aperture.name == aperture
            )
        if lightcurve_type is None:
            join_conditions.append(
                best_lc.lightcurve_type_id == lc.lightcurve_type_id
            )
        else:
            id_q = id_q.join(
                lc.lightcurve_type
            )
            filter_conditions.append(
                LightcurveType.name == lightcurve_type
            )

        id_q = id_q.where(*filter_conditions)
        ids = [id for id, in self.execute(id_q)]

        return self.get_lightpoint_array(ids, columns)
