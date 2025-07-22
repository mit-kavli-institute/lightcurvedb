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

    Extending with single-table inheritance (simple approach):

    >>> class TESSQualityFlags(QualityFlagArray):
    ...     \"\"\"TESS-specific quality flags with known bit definitions.\"\"\"
    ...     __mapper_args__ = {
    ...         "polymorphic_identity": "tess_quality",
    ...     }
    ...     @property
    ...     def cosmic_ray_events(self):
    ...         \"\"\"Return mask of cosmic ray events (bit 0).\"\"\"
    ...         flags = np.array(self.quality_flags, dtype=np.int32)
    ...         return (flags & 1) != 0
    ...     @property
    ...     def saturated_pixels(self):
    ...         \"\"\"Return mask of saturated pixels (bit 1).\"\"\"
    ...         flags = np.array(self.quality_flags, dtype=np.int32)
    ...         return (flags & 2) != 0
    ...     @property
    ...     def spacecraft_anomaly(self):
    ...         \"\"\"Return mask of spacecraft anomalies (bit 4).\"\"\"
    ...         flags = np.array(self.quality_flags, dtype=np.int32)
    ...         return (flags & 16) != 0

    Extending with joined-table inheritance (advanced approach)::

        class SpectroscopicQualityFlags(QualityFlagArray):
            \"\"\"Quality flags for spectroscopic observations.\"\"\"

            __tablename__ = "spectroscopic_quality_flags"
            __mapper_args__ = {
                "polymorphic_identity": "spectroscopic_quality",
            }

            # Primary key also serves as foreign key to parent table
            id = orm.mapped_column(
                sa.ForeignKey("quality_flag_array.id"), primary_key=True
            )

            # Additional columns specific to spectroscopic data
            wavelength_calibration_quality = orm.mapped_column(
                sa.types.Float,
                comment="Wavelength calibration quality score (0-1)"
            )
            spectral_resolution = orm.mapped_column(
                sa.types.Float,
                comment="Actual spectral resolution achieved"
            )
            calibration_lamp_id = orm.mapped_column(
                sa.ForeignKey("calibration_lamp.id"), nullable=True
            )

            @property
            def wavelength_drift(self):
                \"\"\"Return mask of wavelength drift (bit 8).\"\"\"
                flags = np.array(self.quality_flags, dtype=np.int32)
                return (flags & 256) != 0

        class PhotometricQualityFlags(QualityFlagArray):
            \"\"\"Quality flags for photometric observations.\"\"\"

            __tablename__ = "photometric_quality_flags"
            __mapper_args__ = {
                "polymorphic_identity": "photometric_quality",
            }

            id = orm.mapped_column(
                sa.ForeignKey("quality_flag_array.id"), primary_key=True
            )

            # Photometry-specific metadata
            sky_background_level = orm.mapped_column(
                sa.types.Float,
                comment="Median sky background in counts"
            )
            fwhm = orm.mapped_column(
                sa.types.Float,
                comment="Full width at half maximum of PSF"
            )
            extinction_coefficient = orm.mapped_column(
                sa.types.Float, nullable=True
            )

    Polymorphic querying examples:

    >>> # Query all quality flags for an observation
    >>> all_flags = session.query(QualityFlagArray).filter_by(
    ...     observation_id=12345
    ... ).all()

    >>> # Query only TESS quality flags
    >>> tess_flags = session.query(TESSQualityFlags).filter_by(
    ...     observation_id=12345
    ... ).all()

    >>> # Use with_polymorphic for efficient joined loading
    >>> from sqlalchemy.orm import with_polymorphic
    >>>
    >>> poly_flags = with_polymorphic(
    ...     QualityFlagArray,
    ...     [SpectroscopicQualityFlags, PhotometricQualityFlags]
    ... )
    >>> query = session.query(poly_flags).filter(
    ...     poly_flags.observation_id == 12345
    ... )
    >>>
    >>> # Access subclass-specific attributes without additional queries
    >>> for flag in query:
    ...     if isinstance(flag, SpectroscopicQualityFlags):
    ...         print(f"Spectral resolution: {flag.spectral_resolution}")
    ...     elif isinstance(flag, PhotometricQualityFlags):
    ...         print(f"Sky background: {flag.sky_background_level}")

    >>> # Filter by polymorphic type
    >>> spectro_only = session.query(QualityFlagArray).filter_by(
    ...     type="spectroscopic_quality"
    ... ).all()

    Notes
    -----
    The combination of (type, observation_id, target_id) must be unique,
    preventing duplicate quality flag arrays for the same context. NULL
    values in target_id are treated as equal, so only one observation-wide
    quality flag array (with NULL target_id) is allowed per type and
    observation_id combination.

    Quality flag bit definitions are mission and type-specific. Subclasses
    should document their specific bit meanings and may add helper methods
    for flag interpretation.

    See Also
    --------
    Observation : Parent observation model
    Target : Associated target for target-specific flags
    """

    __tablename__ = "quality_flag_array"
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


# Create unique index that treats NULL target_id values as equal
# This ensures only one quality flag array per (type, observation_id,
# target_id) combination. Using COALESCE with -1 as sentinel value for
# NULL target_id to enforce uniqueness
sa.Index(
    "distinct_quality_flags",
    QualityFlagArray.type,
    QualityFlagArray.observation_id,
    sa.func.coalesce(QualityFlagArray.target_id, -1),
    unique=True,
)
