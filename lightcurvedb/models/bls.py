from math import isnan, sqrt

import pyticdb
import sqlalchemy as sa
from astropy import units as u
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION, JSONB
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship
from sqlalchemy.schema import Index

from lightcurvedb.core.base_model import (
    CreatedOnMixin,
    NameAndDescriptionMixin,
    QLPModel,
)


class BLSTagAssociationTable(QLPModel):
    __tablename__ = "bls_association_table"

    id = sa.Column(sa.BigInteger, primary_key=True)
    bls = sa.Column(sa.ForeignKey("bls.id"))
    tag = sa.Column(sa.ForeignKey("bls_tags.id"))


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

    tags = relationship("BLSTag", secondary=BLSTagAssociationTable)

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

        star_radius, star_radius_error = pyticdb.query_by_id(
            bls_result["tic"], "rad", "e_rad"
        )[0]

        if star_radius is None or isnan(star_radius):
            planet_radius = float("nan")
            planet_radius_error = float("nan")
        else:
            star_radius *= u.solRad
            star_radius_error *= u.solRad

            planet_radius = star_radius * sqrt(bls_result["dep"])
            planet_radius_error = star_radius_error * sqrt(bls_result["dep"])

            planet_radius = planet_radius.to(u.earthRad).value
            planet_radius_error = planet_radius.to(u.earthRad).value

        return cls(
            tic_id=bls_result["tic"],
            tce_n=bls_result["planetno"],
            period=bls_result["per"],
            transit_depth=bls_result["dep"],
            transit_duration=bls_result["dur"],
            planet_radius=planet_radius,
            planet_radius_error=planet_radius_error,
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


class BLSTag(QLPModel, CreatedOnMixin, NameAndDescriptionMixin):
    __tablename__ = "bls_tags"
    id = sa.Column(sa.Integer, primary_key=True)

    bls_runs = relationship("BLS", secondary=BLSTagAssociationTable)

    __table_args__ = (
        sa.UniqueConstraint("name"),
        Index("bls_tags_name_idx", "name", postgresql_using="gin"),
    )
