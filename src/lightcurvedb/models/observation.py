import uuid
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
    """
    Base class for astronomical observations.

    Observation is a polymorphic base class that represents a collection
    of measurements taken by an instrument. Subclasses can specialize
    this for different observation types while maintaining a common interface.

    This design allows mission-specific observations (e.g., TESSObservation,
    HSTObservation) to extend the base model with mission-specific fields
    while sharing common functionality.

    Attributes
    ----------
    id : int
        Primary key identifier
    type : str
        Polymorphic discriminator for subclass type
    cadence_reference : ndarray[int64]
        Array of cadence numbers for time ordering
    instrument_id : uuid.UUID
        Foreign key to the instrument used
    instrument : Instrument
        The instrument that made this observation
    interpretations : list[Interpretation]
        Processed versions of this observation
    target_specific_times : list[TargetSpecificTime]
        Target-specific time corrections

    Examples
    --------
    Creating a mission-specific observation subclass:

    >>> class TESSObservation(Observation):
    ...     __mapper_args__ = {
    ...         "polymorphic_identity": "tess_observation",
    ...     }
    ...     sector: Mapped[int]
    ...     orbit_number: Mapped[int]

    Notes
    -----
    This is a polymorphic base class using single table inheritance.
    The 'type' field determines the specific observation subclass.
    Mission-specific fields should be added via subclassing, not by
    modifying this base class.
    """

    __tablename__ = "observation"
    __mapper_args__ = {
        "polymorphic_identity": "observation",
        "polymorphic_on": "type",
    }

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    type: orm.Mapped[str] = orm.mapped_column(index=True)
    cadence_reference: orm.Mapped[npt.NDArray[np.int64]]
    instrument_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
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
    """
    Time series data specific to a target-observation pair.

    This model stores barycentric-corrected time values that account
    for the specific position of a target. It serves as a junction
    between Target and Observation with additional time data.

    Attributes
    ----------
    id : int
        Primary key identifier
    target_id : int
        Foreign key to the target
    observation_id : int
        Foreign key to the observation
    barycentric_julian_dates : ndarray[float64]
        Array of barycentric Julian dates corrected for target position
    target : Target
        The astronomical target
    observation : Observation
        The observation these times correspond to

    Notes
    -----
    Barycentric correction accounts for Earth's motion around the
    solar system barycenter, providing consistent timing for
    astronomical observations.
    """

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
