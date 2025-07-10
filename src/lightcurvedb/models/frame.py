import pathlib
import typing

import sqlalchemy as sa
from sqlalchemy import orm

from lightcurvedb.core.base_model import CreatedOnMixin, LCDBModel


class FITSFrame(LCDBModel, CreatedOnMixin):
    __tablename__ = "fits_frame"
    __mapper_args__ = {
        "polymorphic_identity": "basefits",
        "polymorphic_on": "type",
    }

    __table_args__ = (
        sa.UniqueConstraint(
            "type",
            "observation_id",
            "cadence",
            name="distinct_frame_observation_idx",
        ),
    )

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    type: orm.Mapped[str]
    observation_id: orm.Mapped[int] = orm.mapped_column(
        sa.ForeignKey("observation.id")
    )

    cadence: orm.Mapped[int] = orm.mapped_column(sa.BigInteger)

    # Define primary keywords
    simple: orm.Mapped[bool]
    bitpix: orm.Mapped[int]
    naxis: orm.Mapped[int]

    naxis_values: orm.Mapped[list[int]] = orm.mapped_column(
        sa.ARRAY(sa.Integer),
        comment="Representation of the required NAXIS[n] keywords",
    )
    extend: orm.Mapped[bool]
    bscale: orm.Mapped[float] = orm.mapped_column(
        default=1.0, comment="Physical Value = BZERO + BSCALE * stored_value"
    )
    bzero: orm.Mapped[float] = orm.mapped_column(
        default=0.0, comment="Physical Value = BZERO + BSCALE * stored_value"
    )
    file_path: orm.Mapped[typing.Optional[pathlib.Path]]
