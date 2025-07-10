import uuid

import numpy as np
import sqlalchemy as sa
from numpy import typing as npt
from sqlalchemy import orm

from lightcurvedb.core.base_model import LCDBModel


class Observation(LCDBModel):
    __tablename__ = "observation"
    __mapper_args__ = {
        "polymorphic_identity": "observation",
        "polymorphic_on": "type",
    }

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    type: orm.Mapped[str]
    instrument_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.ForeignKey("instrument.id", ondelete="CASCADE")
    )
    cadence_reference: orm.Mapped[npt.NDArray[np.int64]]


class TargetSpecificTime(LCDBModel):
    __tablename__ = "target_specific_time"

    id: orm.Mapped[int] = orm.mapped_column(sa.BigInteger, primary_key=True)
    target_id: orm.Mapped[int] = orm.mapped_column(sa.ForeignKey("target.id"))
    observation_id: orm.Mapped[int] = orm.mapped_column(
        sa.ForeignKey(Observation.id)
    )

    barycentric_julian_dates: orm.Mapped[npt.NDArray[np.float64]]
