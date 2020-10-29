from sqlalchemy import Column, BigInteger, Integer, Float, String, ForeignKey
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.dialects.postgresql import JSONB, DOUBLE_PRECISION
from sqlalchemy.orm import relationship
from lightcurvedb.core.base_model import QLPDataProduct


class BLS(QLPDataProduct):

    __tablename__ = "bls"

    id = Column(
        BigInteger,
        primary_key=True
    )
    lightcurve_id = Column(
        ForeignKey(
            "lightcurves.id", onupdate="CASCADE", ondelete="CASCADE"
        ),
        index=True
    )
    astronet_score = Column(Float, nullable=True, index=True)
    astronet_version = Column(String(256), nullable=True)
    runtime_parameters = Column(JSONB, nullable=False)

    # Begin Astrophysical parameters
    period = Column(DOUBLE_PRECISION, nullable=False, index=True)  # Days
    transit_duration = Column(DOUBLE_PRECISION, nullable=False)  # Days
    planet_radius = Column(DOUBLE_PRECISION, nullable=False)  # Earth Radii
    planet_radius_error = Column(DOUBLE_PRECISION, nullable=False)  # Earth Radii

    # Begin BLS info
    points_pre_transit = Column(Integer, nullable=False)
    points_in_transit = Column(Integer, nullable=False)
    points_post_transit = Column(Integer, nullable=False)
    transits = Column(Integer, nullable=False, index=True)
    transit_shape = Column(DOUBLE_PRECISION, nullable=False)
    duration_rel_period = Column(DOUBLE_PRECISION, nullable=False)
    rednoise = Column(DOUBLE_PRECISION, nullable=False)
    whitenoise = Column(DOUBLE_PRECISION, nullable=False)
    signal_to_noise = Column(DOUBLE_PRECISION, nullable=False)
    signal_to_pinknoise = Column(DOUBLE_PRECISION, nullable=False)
    sde = Column(DOUBLE_PRECISION, nullable=False)
    sr = Column(DOUBLE_PRECISION, nullable=False)
    period_inv_transit = Column(DOUBLE_PRECISION, nullable=False)

    # Begin relationship logic
    lightcurve = relationship(
        "Lightcurve",
        back_populates="bls_results"
    )

    @hybrid_property
    def qingress(self):
        return self.transit_shape

    @qingress.expression
    def qingress(cls):
        return cls.transit_shape

    @hybrid_property
    def qtran(self):
        return self.duration_rel_period

    @qtran.expression
    def qtran(cls):
        return cls.duration_rel_period

    @hybrid_property
    def snr(self):
        return self.signal_to_noise

    @snr.expression
    def snr(cls):
        return cls.signal_to_noise

    @hybrid_property
    def spnr(self):
        return self.signal_to_pinknoise

    @spnr.expression
    def spnr(cls):
        return cls.signal_to_pinknoise

    @hybrid_property
    def tc(self):
        return self.transit_center

    @tc.expression
    def tc(cls):
        return cls.transit_center
