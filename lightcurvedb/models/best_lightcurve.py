import sqlalchemy as sa
from sqlalchemy.orm import relationship
from sqlalchemy.schema import UniqueConstraint

from lightcurvedb.core.base_model import CreatedOnMixin, QLPModel


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

    id = sa.Column(sa.BigInteger, primary_key=True)
    tic_id = sa.Column(sa.BigInteger, nullable=False, index=True)

    aperture_id = sa.Column(
        sa.ForeignKey("apertures.id", ondelete="RESTRICT"),
        index=True,
    )
    lightcurve_type_id = sa.Column(
        sa.ForeignKey("lightcurvetypes.id", ondelete="RESTRICT"),
        index=True,
    )
    orbit_id = sa.Column(
        sa.ForeignKey("orbits.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )

    aperture = relationship("Aperture")
    lightcurve_type = relationship("LightcurveType")
    orbit = relationship("Orbit")

    @classmethod
    def lightcurve_join(cls, OtherLightcurve):
        return sa.and_(
            cls.tic_id == OtherLightcurve.tic_id,
            cls.aperture_id == OtherLightcurve.aperture_id,
            cls.lightcurve_type_id == OtherLightcurve.lightcurve_type_id,
            cls.orbit_id == OtherLightcurve.orbit_id,
        )
