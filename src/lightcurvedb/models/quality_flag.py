from typing import TYPE_CHECKING

import numpy as np
import sqlalchemy as sa
from numpy import typing as npt
from sqlalchemy import orm

from lightcurvedb.core.base_model import CreatedOnMixin, LCDBModel

if TYPE_CHECKING:
    from lightcurvedb.models.observation import Observation
    from lightcurvedb.models.target import Target


class QualityFlagArray(LCDBModel, CreatedOnMixin):
    """
    Stores quality flag arrays for astronomical observations.

    QualityFlagArray represents bit-encoded quality information for
    time-series astronomical data. Each element in the array corresponds
    to a cadence in the observation, with individual bits representing
    different quality conditions or data issues.

    This is a polymorphic base class that can be extended for
    mission-specific quality flag implementations with specialized
    bit definitions.

    Parameters
    ----------
    type : str
        Quality flag type identifier (e.g., 'pixel_quality', 'cosmic_ray')
    observation_id : int
        Foreign key to the parent observation
    target_id : int, optional
        Foreign key to a specific target when flags are target-specific
    quality_flags : ndarray[int32]
        Array of 32-bit integers where each bit represents a quality condition

    Attributes
    ----------
    id : int
        Primary key identifier
    type : str
        Polymorphic discriminator and quality flag category
    observation_id : int
        Reference to the parent observation
    target_id : int or None
        Reference to specific target (null for observation-wide flags)
    quality_flags : ndarray[int32]
        Bit-encoded quality flag array
    observation : Observation
        Parent observation relationship
    target : Target or None
        Target relationship when flags are target-specific
    created_on : datetime
        Timestamp of record creation (from CreatedOnMixin)

    Examples
    --------
    Creating observation-wide quality flags:

    >>> obs_flags = QualityFlagArray(
    ...     observation_id=12345,
    ...     quality_flags=np.array([0, 1, 4, 5], dtype=np.int32)
    ... )

    Creating target-specific quality flags:

    >>> target_flags = QualityFlagArray(
    ...     observation_id=12345,
    ...     target_id=67890,
    ...     quality_flags=np.array([0, 0, 2, 8], dtype=np.int32)
    ... )

    Interpreting bit flags:

    >>> # Bit 0: Cosmic ray
    >>> # Bit 1: Saturation
    >>> # Bit 2: Bad pixel
    >>> cosmic_ray_mask = (flags.quality_flags & 1) != 0
    >>> saturated_mask = (flags.quality_flags & 2) != 0

    Notes
    -----
    The combination of (type, observation_id, target_id) must be unique,
    preventing duplicate quality flag arrays for the same context.

    Quality flag bit definitions are mission and type-specific. Subclasses
    should document their specific bit meanings and may add helper methods
    for flag interpretation.

    See Also
    --------
    Observation : Parent observation model
    Target : Associated target for target-specific flags
    """

    __tablename__ = "quality_flag_array"
    __table_args__ = (
        sa.UniqueConstraint(
            "type",
            "observation_id",
            "target_id",
            name="distinct_quality_flags",
        ),
    )
    __mapper_args__ = {
        "polymorphic_identity": "base_quality_flag",
        "polymorphic_on": "type",
    }

    # Primary key - uses BigInteger for large datasets
    id: orm.Mapped[int] = orm.mapped_column(sa.BigInteger, primary_key=True)
    type: orm.Mapped[str] = orm.mapped_column(index=True)

    # Foreign key to parent observation - cascades on delete
    observation_id: orm.Mapped[int] = orm.mapped_column(
        sa.ForeignKey("observation.id", ondelete="CASCADE"), index=True
    )

    # Optional foreign key to specific target - null for observation-wide flags
    target_id: orm.Mapped[int] = orm.mapped_column(
        sa.ForeignKey("target.id"), index=True, nullable=True
    )

    # Array of 32-bit integers where each bit represents a quality condition
    # Length should match the observation's cadence array length
    quality_flags: orm.Mapped[npt.NDArray[np.int32]]

    observation: orm.Mapped["Observation"] = orm.relationship(
        "Observation", back_populates="quality_flag_arrays"
    )
    target: orm.Mapped["Target | None"] = orm.relationship(
        "Target", back_populates="quality_flag_arrays"
    )
