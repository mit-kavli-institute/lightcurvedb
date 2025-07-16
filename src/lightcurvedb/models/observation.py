from typing import TYPE_CHECKING

import numpy as np
import sqlalchemy as sa
from numpy import typing as npt
from sqlalchemy import orm

from lightcurvedb.core.base_model import LCDBModel

if TYPE_CHECKING:
    from lightcurvedb.models.instrument import Instrument
    from lightcurvedb.models.interpretation import Interpretation
    from lightcurvedb.models.target import Target


class Observation(LCDBModel):
    __tablename__ = "observation"
    __mapper_args__ = {
        "polymorphic_identity": "observation",
        "polymorphic_on": "type",
    }

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    type: orm.Mapped[str] = orm.mapped_column(index=True)
    cadence_reference: orm.Mapped[npt.NDArray[np.int64]]
    instrument_id: orm.Mapped[int] = orm.mapped_column(
        sa.ForeignKey("instrument.id", ondelete="CASCADE")
    )

    instrument: orm.Mapped["Instrument"] = orm.relationship(
        "Instrument", back_populates="observations"
    )

    interpretations: orm.Mapped[list["Interpretation"]] = orm.relationship(
        "Interpretation", back_populates="observation"
    )
    target_specific_times: orm.Mapped[
        list["TargetSpecificTime"]
    ] = orm.relationship(back_populates="observation")


class TargetSpecificTime(LCDBModel):
    __tablename__ = "target_specific_time"

    id: orm.Mapped[int] = orm.mapped_column(sa.BigInteger, primary_key=True)
    target_id: orm.Mapped[int] = orm.mapped_column(
        sa.ForeignKey("target.id"), index=True
    )
    observation_id: orm.Mapped[int] = orm.mapped_column(
        sa.ForeignKey(Observation.id, ondelete="CASCADE"), index=True
    )

    barycentric_julian_dates: orm.Mapped[npt.NDArray[np.float64]]

    # Relationships
    target: orm.Mapped["Target"] = orm.relationship(
        back_populates="target_specific_times"
    )
    observation: orm.Mapped["Observation"] = orm.relationship(
        back_populates="target_specific_times"
    )
