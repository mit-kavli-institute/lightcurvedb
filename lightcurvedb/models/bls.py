from lightcurvedb.core.base_model import QLPDataProduct
from lightcurvedb.models.lightcurve import Lightcurve
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Float,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
)
from click import Choice
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION, JSONB, insert
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship
from sqlalchemy.schema import Index


class BLS(QLPDataProduct):

    __tablename__ = "bls"

    id = Column(BigInteger, primary_key=True)
    sector = Column(Integer, index=True, nullable=False)
    lightcurve_id = Column(
        ForeignKey("lightcurves.id", onupdate="CASCADE", ondelete="CASCADE"),
        index=True,
    )
    astronet_score = Column(Float, nullable=True, index=True)
    astronet_version = Column(String(256), nullable=True)
    runtime_parameters = Column(
        JSONB,
        nullable=False,
        index=Index(name="runtime_parameters_gin", postgresql_using="gin"),
    )

    # Begin Astrophysical parameters
    period = Column(DOUBLE_PRECISION, nullable=False, index=True)  # Days
    transit_depth = Column(DOUBLE_PRECISION, nullable=False)
    transit_duration = Column(DOUBLE_PRECISION, nullable=False)  # Days
    planet_radius = Column(
        DOUBLE_PRECISION, nullable=False, index=True
    )  # Earth Radii
    planet_radius_error = Column(
        DOUBLE_PRECISION, nullable=False
    )  # Earth Radii

    # Begin BLS info
    points_pre_transit = Column(Integer, nullable=False)
    points_in_transit = Column(Integer, nullable=False)
    points_post_transit = Column(Integer, nullable=False)
    transits = Column(Integer, nullable=False, index=True)
    transit_shape = Column(DOUBLE_PRECISION, nullable=False)
    transit_center = Column(DOUBLE_PRECISION, nullable=False)
    duration_rel_period = Column(DOUBLE_PRECISION, nullable=False)
    rednoise = Column(DOUBLE_PRECISION, nullable=False)
    whitenoise = Column(DOUBLE_PRECISION, nullable=False)
    signal_to_noise = Column(DOUBLE_PRECISION, nullable=False, index=True)
    signal_to_pinknoise = Column(DOUBLE_PRECISION, nullable=False)
    sde = Column(DOUBLE_PRECISION, nullable=False)
    sr = Column(DOUBLE_PRECISION, nullable=False)
    period_inv_transit = Column(DOUBLE_PRECISION, nullable=False)

    # Constraints
    __table_args__ = (
        UniqueConstraint(
            "lightcurve_id", "created_on", name="unique_bls_runtime"
        ),
    )

    # Begin relationship logic
    lightcurve = relationship("Lightcurve", back_populates="bls_results")

    # Click queryable parameters
    click_parameters = Choice(
        [
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
            cls.runtime_parameters["legacy"].cast(Boolean).label("is_legacy")
        )

    @hybrid_property
    def tic_id(self):
        return self.lightcurve.tic_id

    @tic_id.expression
    def tic_id(cls):
        return Lightcurve.tic_id

    @classmethod
    def upsert_q(cls):
        update_params = [
            "astronet_score",
            "astronet_version",
            "runtime_parameters",
            "period",
            "transit_depth",
            "planet_radius",
            "planet_radius_error",
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
            "sector",
        ]

        q = insert(cls.__table__)
        q = q.on_conflict_do_update(
            constraint="unique_bls_runtime",
            set_={
                param: getattr(q.excluded, param, None)
                for param in update_params
            },
        )
        return q
