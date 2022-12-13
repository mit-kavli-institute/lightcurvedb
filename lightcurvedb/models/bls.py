import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION, JSONB
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.schema import Index

from lightcurvedb.core.base_model import CreatedOnMixin, QLPModel


class BLS(QLPModel, CreatedOnMixin):

    __tablename__ = "bls"

    id = sa.Column(sa.BigInteger, primary_key=True)
    sector = sa.Column(sa.SmallInteger, index=True)
    tic_id = sa.Column(sa.BigInteger, index=True)

    tce_n = sa.Column(sa.SmallInteger, nullable=False, index=True)

    # Begin Astrophysical parameters
    transit_period = sa.Column(
        DOUBLE_PRECISION, nullable=False, index=True
    )  # Days
    transit_depth = sa.Column(DOUBLE_PRECISION, nullable=False)
    transit_duration = sa.Column(DOUBLE_PRECISION, nullable=False)  # Days
    planet_radius = sa.Column(
        DOUBLE_PRECISION, nullable=False, index=True
    )  # Earth Radii
    planet_radius_error = sa.Column(
        DOUBLE_PRECISION, nullable=False
    )  # Earth Radii

    # Begin BLS info
    points_pre_transit = sa.Column(sa.Integer, nullable=False)
    points_in_transit = sa.Column(sa.Integer, nullable=False)
    points_post_transit = sa.Column(sa.Integer, nullable=False)
    out_of_transit_magnitude = sa.Column(DOUBLE_PRECISION, nullable=False)
    transits = sa.Column(sa.Integer, nullable=False, index=True)
    ingress = sa.Column(DOUBLE_PRECISION, nullable=False)
    transit_center = sa.Column(DOUBLE_PRECISION, nullable=False)
    rednoise = sa.Column(DOUBLE_PRECISION, nullable=False)
    whitenoise = sa.Column(DOUBLE_PRECISION, nullable=False)
    signal_to_noise = sa.Column(DOUBLE_PRECISION, nullable=False, index=True)
    signal_to_pinknoise = sa.Column(DOUBLE_PRECISION, nullable=False)
    signal_detection_efficiency = sa.Column(DOUBLE_PRECISION, nullable=False)
    signal_residual = sa.Column(DOUBLE_PRECISION, nullable=False)
    zero_point_transit = sa.Column(DOUBLE_PRECISION, nullable=False)

    metadata = sa.Column(JSONB, default={})

    __table_args__ = (
        Index("bls_metadata_idx", metadata, postgresql_using="gin"),
    )

    @hybrid_property
    def duration_rel_period(self):
        return self.transit_duration / self.transit_period

    @duration_rel_period.expression
    def duration_rel_period(cls):
        return cls.transit_duration / cls.transit_period

    @hybrid_property
    def qingress(self):
        return self.transit_shape

    @qingress.expression
    def qingress(cls):
        return cls.transit_shape.label("qingress")

    @hybrid_property
    def qtran(self):
        return self.duration_rel_period

    @qtran.expression
    def qtran(cls):
        return cls.duration_rel_period.label("qtran")

    @hybrid_property
    def snr(self):
        return self.signal_to_noise

    @snr.expression
    def snr(cls):
        return cls.signal_to_noise.label("snr")

    @hybrid_property
    def spnr(self):
        return self.signal_to_pinknoise

    @spnr.expression
    def spnr(cls):
        return cls.signal_to_pinknoise.label("spnr")

    @hybrid_property
    def tc(self):
        return self.transit_center

    @tc.expression
    def tc(cls):
        return cls.transit_center.label("tc")

    @classmethod
    def from_bls_result(cls, bls_result):
        return cls(
            tic_id=bls_result["tic"],
            tce_n=bls_result["planetno"],
            period=bls_result["per"],
            transit_depth=bls_result["dep"],
            transit_duration=bls_result["dur"],
            planet_radius=None,
            planet_radius_error=None,
            points_pre_transit=bls_result["nbefore"],
            points_in_transit=bls_result["nt"],
            points_post_transit=bls_result["nafter"],
            out_of_transit_magnitude=bls_result["ootmag"],
            transits=bls_result["Nt"],
            ingress=bls_result["qin"] * bls_result["dur"],
            transit_center=bls_result["epo"],
            rednoise=bls_result["sig_r"],
            whitenoise=bls_result["sig_w"],
            signal_to_noise=bls_result["sn"],
            signal_to_pinknoise=bls_result["spn"],
            signal_detection_efficiency=bls_result["sde"],
            signal_residual=bls_result["sr"],
            zero_point_transit=bls_result["zpt"],
        )
