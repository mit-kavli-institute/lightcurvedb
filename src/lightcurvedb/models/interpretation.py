import typing
from typing import TYPE_CHECKING

import numpy as np
import sqlalchemy as sa
from numpy import typing as npt
from sqlalchemy import orm

from lightcurvedb.core.base_model import LCDBModel, NameAndDescriptionMixin

if TYPE_CHECKING:
    from lightcurvedb.models.observation import Observation
    from lightcurvedb.models.target import Target


class PhotometricSource(LCDBModel, NameAndDescriptionMixin):
    __tablename__ = "photometric_source"

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)

    processing_groups: orm.Mapped[list["ProcessingGroup"]] = orm.relationship(
        "ProcessingGroup", back_populates="photometric_source"
    )


class DetrendingMethod(LCDBModel, NameAndDescriptionMixin):
    __tablename__ = "detrending_method"

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    processing_groups: orm.Mapped[list["ProcessingGroup"]] = orm.relationship(
        "ProcessingGroup", back_populates="detrending_method"
    )


class ProcessingGroup(LCDBModel, NameAndDescriptionMixin):
    __tablename__ = "processing_group"
    __table_args__ = (
        sa.UniqueConstraint("photometric_source_id", "detrending_method_id"),
    )

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)

    photometric_source_id: orm.Mapped[int] = orm.mapped_column(
        sa.ForeignKey(PhotometricSource.id)
    )
    detrending_method_id: orm.Mapped[int] = orm.mapped_column(
        sa.ForeignKey(DetrendingMethod.id)
    )

    interpretations: orm.Mapped[list["Interpretation"]] = orm.relationship(
        "Interpretation", back_populates="processing_group"
    )
    photometric_source: orm.Mapped[PhotometricSource] = orm.relationship(
        PhotometricSource, back_populates="processing_groups"
    )
    detrending_method: orm.Mapped[DetrendingMethod] = orm.relationship(
        DetrendingMethod, back_populates="processing_groups"
    )


class Interpretation(LCDBModel):
    __tablename__ = "interpretation"

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    processing_group_id: orm.Mapped[int] = orm.mapped_column(
        sa.ForeignKey(ProcessingGroup.id, ondelete="CASCADE"), index=True
    )
    target_id: orm.Mapped[int] = orm.mapped_column(
        sa.ForeignKey("target.id", ondelete="CASCADE"), index=True
    )
    observation_id: orm.Mapped[int] = orm.mapped_column(
        sa.ForeignKey("observation.id", ondelete="CASCADE"), index=True
    )

    values = orm.Mapped[npt.NDArray[np.float64]]
    errors = orm.Mapped[typing.Optional[npt.NDArray[np.float64]]]

    # Relationships
    processing_group: orm.Mapped["ProcessingGroup"] = orm.relationship(
        back_populates="interpretations"
    )
    target: orm.Mapped["Target"] = orm.relationship(
        back_populates="interpretations"
    )
    observation: orm.Mapped["Observation"] = orm.relationship(
        back_populates="interpretations"
    )
