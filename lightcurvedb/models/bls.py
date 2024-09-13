from decimal import Decimal
from math import sqrt
from typing import Any

import pyticdb
import sqlalchemy as sa
from astropy import units as u
from sqlalchemy.dialects.postgresql import REGCONFIG
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import Mapped, mapped_column, relationship
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

    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True)
    sector: Mapped[int] = mapped_column(sa.SmallInteger, index=True)
    tic_id: Mapped[int] = mapped_column(sa.BigInteger, index=True)

    tce_n: Mapped[int] = mapped_column(sa.SmallInteger, index=True)

    # Begin Astrophysical parameters
    transit_period: Mapped[Decimal] = mapped_column(index=True)  # Days
    transit_depth: Mapped[Decimal]
    transit_duration: Mapped[Decimal]  # Days
    planet_radius: Mapped[Decimal] = mapped_column(index=True)  # Earth Radii
    planet_radius_error: Mapped[Decimal]

    # Begin BLS info
    points_pre_transit: Mapped[int]
    points_in_transit: Mapped[int]
    points_post_transit: Mapped[int]
    out_of_transit_magnitude: Mapped[Decimal]
    transits: Mapped[Decimal] = mapped_column(index=True)
    ingress: Mapped[Decimal]
    transit_center: Mapped[Decimal]
    rednoise: Mapped[Decimal]
    whitenoise: Mapped[Decimal]
    signal_to_noise: Mapped[Decimal] = mapped_column(index=True)
    signal_to_pinknoise: Mapped[Decimal]
    signal_detection_efficiency: Mapped[Decimal]
    signal_residual: Mapped[Decimal]
    zero_point_transit: Mapped[Decimal]

    additional_data: Mapped[dict[str, Any]] = mapped_column(default={})

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
        return self.ingress / self.transit_duration

    @qingress.expression
    def qingress(cls):
        return cls.ingress / cls.transit_duration

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
