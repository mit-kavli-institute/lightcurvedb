import typing

import numpy as np
import sqlalchemy as sa
from numpy import typing as npt
from sqlalchemy import orm

from lightcurvedb.core.base_model import LCDBModel, NameAndDescriptionMixin


class InterpretationType(LCDBModel, NameAndDescriptionMixin):
    __tablename__ = "interpretation_type"

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)


class ProcessingGroup(LCDBModel, NameAndDescriptionMixin):
    __tablename__ = "processing_group"

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)


class InterpretationAssociationTable(LCDBModel):
    __tablename__ = "interp_association"
    __table_args__ = (
        sa.UniqueConstraint("previous_type_id", "next_type_id", "group_id"),
    )

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    previous_type_id: orm.Mapped[int] = orm.mapped_column(
        sa.ForeignKey(InterpretationType.id)
    )
    next_type_id: orm.Mapped[int] = orm.mapped_column(
        sa.ForeignKey(InterpretationType.id)
    )
    group_id: orm.Mapped[int] = orm.mapped_column(
        sa.ForeignKey(ProcessingGroup.id), index=True
    )


class Interpretation(LCDBModel):
    __tablename__ = "interpretation"

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    processing_group_id: orm.Mapped[int] = orm.mapped_column(
        sa.ForeignKey(ProcessingGroup.id, index=True)
    )
    target_id: orm.Mapped[int] = orm.mapped_column(
        sa.ForeignKey("target.id"), index=True
    )
    observation_id: orm.Mapped[int] = orm.mapped_column(
        sa.ForeignKey("observation.id"), index=True
    )

    values = orm.Mapped[npt.NDArray[np.float64]]
    errors = orm.Mapped[typing.Optional[npt.NDArray[np.float64]]]
