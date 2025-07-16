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
            "cadence",
            name="distinct_frame_type_cadence_idx",
        ),
    )

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    type: orm.Mapped[str] = orm.mapped_column(index=True)
    cadence: orm.Mapped[int] = orm.mapped_column(sa.BigInteger, index=True)

    observation_id: orm.Mapped[int] = orm.mapped_column(
        sa.ForeignKey("observation.id", ondelete="CASCADE"), index=True
    )

    # Define primary keywords
    simple: orm.Mapped[bool]
    bitpix: orm.Mapped[int]
    naxis: orm.Mapped[int]

    naxis_values: orm.Mapped[list[int]] = orm.mapped_column(
        sa.ARRAY(sa.Integer),
        comment="Representation of the required NAXIS[n] keywords",
    )
    extended: orm.Mapped[bool]
    bscale: orm.Mapped[float] = orm.mapped_column(
        default=1.0, comment="Physical Value = BZERO + BSCALE * stored_value"
    )
    bzero: orm.Mapped[float] = orm.mapped_column(
        default=0.0, comment="Physical Value = BZERO + BSCALE * stored_value"
    )
    file_path: orm.Mapped[typing.Optional[pathlib.Path]]
