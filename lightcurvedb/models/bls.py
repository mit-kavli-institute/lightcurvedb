import sqlalchemt as sa
from click import Choice
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION, JSONB
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.schema import Index

from lightcurvedb.core.base_model import CreatedOnMixin, QLPModel


class BLS(QLPModel, CreatedOnMixin):

    __tablename__ = "bls"

    id = sa.Column(sa.BigInteger, primary_key=True)
    sector = sa.Column(sa.SmallInteger, index=True)
    tic_id = sa.Column(sa.BigInteger, index=True)

    tce_n = sa.Column(
        sa.SmallInteger,
        index=Index(name="tce_n_gin", postgresql_using="gin"),
        nullable=False,
    )
    runtime_parameters = sa.Column(
        JSONB,
        nullable=False,
        index=Index(name="runtime_parameters_gin", postgresql_using="gin"),
    )

    # Begin Astrophysical parameters
    period = sa.Column(DOUBLE_PRECISION, nullable=False, index=True)  # Days
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
    transits = sa.Column(sa.Integer, nullable=False, index=True)
    transit_shape = sa.Column(DOUBLE_PRECISION, nullable=False)
    transit_center = sa.Column(DOUBLE_PRECISION, nullable=False)
    duration_rel_period = sa.Column(DOUBLE_PRECISION, nullable=False)
    rednoise = sa.Column(DOUBLE_PRECISION, nullable=False)
    whitenoise = sa.Column(DOUBLE_PRECISION, nullable=False)
    signal_to_noise = sa.Column(DOUBLE_PRECISION, nullable=False, index=True)
    signal_to_pinknoise = sa.Column(DOUBLE_PRECISION, nullable=False)
    signal_to_rednoise = sa.Column(DOUBLE_PRECISION, nullable=False)
    signal_detection_efficiency = sa.Column(DOUBLE_PRECISION, nullable=False)
    signal_residual = sa.Column(DOUBLE_PRECISION, nullable=False)
    period_inv_transit = sa.Column(DOUBLE_PRECISION, nullable=False)

    # Click queryable parameters
    click_parameters = Choice(
        [
            "created_on",
            "lightcurve",
            "period",
            "transit_depth",
            "transit_duration",
            "planet_radius",
            "points_pre_transit",
            "points_in_transit",
            "points_post_transit",
            "transits",
            "transit_shape",
            "transit_center",
            "duration_rel_period",
            "rednoise",
            "whitenoise",
            "signal_to_noise",
            "signal_to_pinknoise",
            "sde",
            "sr",
            "period_inv_transit",
            "tic_id",
            "sector",
        ],
        case_sensitive=False,
    )

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

    @hybrid_property
    def is_legacy(self):
        return self.runtime_parameters.get("legacy", False)

    @is_legacy.expression
    def is_legacy(cls):
        return (
            cls.runtime_parameters["legacy"]
            .cast(sa.Boolean)
            .label("is_legacy")
        )
