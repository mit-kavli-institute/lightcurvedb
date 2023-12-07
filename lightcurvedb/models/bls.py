from math import sqrt

import pyticdb
import sqlalchemy as sa
from astropy import units as u
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION, JSONB, REGCONFIG
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship
from sqlalchemy.schema import Index

from lightcurvedb.core.base_model import (
    CreatedOnMixin,
    NameAndDescriptionMixin,
    QLPModel,
)
from lightcurvedb.util import type_check

BLSTagAssociationTable = sa.Table(
    "bls_association_table",
    QLPModel.metadata,
    sa.Column("id", sa.BigInteger, primary_key=True),
    sa.Column("bls_id", sa.ForeignKey("bls.id")),
    sa.Column("tag_id", sa.ForeignKey("bls_tags.id")),
)


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

    additional_data = sa.Column(JSONB, default={})

    tags = relationship(
        "BLSTag", secondary=BLSTagAssociationTable, back_populates="bls_runs"
    )

    __table_args__ = (
        Index(
            "bls_additional_data_idx", additional_data, postgresql_using="gin"
        ),
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
        star_radius = type_check.safe_float(star_radius)
        star_radius_error = type_check.safe_float(star_radius_error)

        star_radius *= u.solRad
        star_radius_error *= u.solRad

        planet_radius = star_radius * sqrt(bls_result["dep"])
        planet_radius_error = star_radius_error * sqrt(bls_result["dep"])

        planet_radius = planet_radius.to(u.earthRad).value
        planet_radius_error = planet_radius_error.to(u.earthRad).value

        return cls(
            tic_id=bls_result["tic"],
            tce_n=bls_result["planetno"],
            transit_period=type_check.sql_nan_cast(bls_result["per"]),
            transit_depth=type_check.sql_nan_cast(bls_result["dep"]),
            transit_duration=type_check.sql_nan_cast(bls_result["dur"]),
            planet_radius=type_check.sql_nan_cast(planet_radius),
            planet_radius_error=type_check.sql_nan_cast(planet_radius_error),
            points_pre_transit=type_check.sql_nan_cast(bls_result["nbefore"]),
            points_in_transit=type_check.sql_nan_cast(bls_result["nt"]),
            points_post_transit=type_check.sql_nan_cast(bls_result["nafter"]),
            out_of_transit_magnitude=type_check.sql_nan_cast(
                bls_result["ootmag"]
            ),
            transits=bls_result["Nt"],
            ingress=type_check.sql_nan_cast(
                bls_result["qin"] * bls_result["dur"]
            ),
            transit_center=type_check.sql_nan_cast(bls_result["epo"]),
            rednoise=type_check.sql_nan_cast(bls_result["sig_r"]),
            whitenoise=type_check.sql_nan_cast(bls_result["sig_w"]),
            signal_to_noise=type_check.sql_nan_cast(bls_result["sn"]),
            signal_to_pinknoise=type_check.sql_nan_cast(bls_result["spn"]),
            signal_detection_efficiency=type_check.sql_nan_cast(
                bls_result["sde"]
            ),
            signal_residual=type_check.sql_nan_cast(bls_result["sr"]),
            zero_point_transit=type_check.sql_nan_cast(bls_result["zpt"]),
        )


class BLSTag(QLPModel, CreatedOnMixin, NameAndDescriptionMixin):
    __tablename__ = "bls_tags"
    id = sa.Column(sa.Integer, primary_key=True)

    bls_runs = relationship(
        "BLS", secondary=BLSTagAssociationTable, back_populates="tags"
    )

    __table_args__ = (
        sa.UniqueConstraint("name"),
        Index(
            "bls_tags_name_tsv",
            sa.func.to_tsvector(
                sa.cast(sa.literal("english"), type_=REGCONFIG), "name"
            ),
            postgresql_using="gin",
        ),
    )
