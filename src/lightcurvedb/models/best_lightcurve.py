import sqlalchemy as sa
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.schema import UniqueConstraint

from lightcurvedb.core.base_model import CreatedOnMixin, QLPModel


class BestOrbitLightcurve(QLPModel, CreatedOnMixin):
    """
    A mapping of lightcurves to orbits to define the best detrending method
    used. This allows a heterogenous mix of lightcurves to coalese into a
    single timeseries.
    """

    __tablename__ = "best_orbit_lightcurves"
    __table_args__ = (
        UniqueConstraint(
            "tic_id",
            "orbit_id",
        ),
    )

    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True)
    tic_id: Mapped[int] = mapped_column(sa.BigInteger, index=True)

    aperture_id: Mapped[int] = mapped_column(
        sa.ForeignKey("apertures.id", ondelete="RESTRICT"),
        index=True,
    )
    small_aperture_id: Mapped[int] = mapped_column(
        sa.ForeignKey("apertures.id", ondelete="RESTRICT"),
        index=True,
    )
    large_aperture_id: Mapped[int] = mapped_column(
        sa.ForeignKey("apertures.id", ondelete="RESTRICT"),
        index=True,
    )
    lightcurve_type_id: Mapped[int] = mapped_column(
        sa.ForeignKey("lightcurvetypes.id", ondelete="RESTRICT"),
        index=True,
    )
    orbit_id: Mapped[int] = mapped_column(
        sa.ForeignKey("orbits.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )

    aperture = relationship("Aperture", foreign_keys=[aperture_id])
    small_aperture = relationship("Aperture", foreign_keys=[small_aperture_id])
    large_aperture = relationship("Aperture", foreign_keys=[large_aperture_id])
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
