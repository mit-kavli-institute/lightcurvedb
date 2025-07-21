from typing import TYPE_CHECKING

import numpy as np
import sqlalchemy as sa
from numpy import typing as npt
from sqlalchemy import orm

from lightcurvedb.core.base_model import CreatedOnMixin, LCDBModel

if TYPE_CHECKING:
    from lightcurvedb.models.observation import Observation


class QualityFlagArray(LCDBModel, CreatedOnMixin):

    __tablename__ = "quality_flag_array"
    __table_args__ = sa.UniqueConstraint(
        "type", "observation_id", "target_id", name="distinct_quality_flags"
    )
    __mapper_args__ = {
        "polymorphic_identity": "base_quality_flag",
        "polymorphic_on": "type",
    }

    id: orm.Mapped[int] = orm.mapped_column(sa.BigInteger, primary_key=True)
    type: orm.Mapped[str] = orm.mapped_column(index=True)
    observation_id: orm.Mapped[int] = orm.mapped_column(
        sa.ForeignKey("observation.id"), index=True
    )
    target_id: orm.Mapped[int] = orm.mapped_column(
        sa.ForeignKey("target.id"), index=True, nullable=True
    )

    quality_flags: orm.Mapped[npt.NDArray[np.int32]]

    observation: orm.Mapped["Observation"] = orm.relationship(
        "Observation", back_populates="quality_flag_arrays"
    )
