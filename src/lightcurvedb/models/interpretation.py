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
    """
    Defines a source or method of photometric measurement.

    PhotometricSource represents different ways of extracting photometric
    data from observations, such as aperture photometry with different
    aperture sizes or PSF photometry.

    Attributes
    ----------
    id : int
        Primary key identifier
    name : str
        Name of the photometric method (inherited from mixin)
    description : str
        Detailed description (inherited from mixin)
    processing_groups : list[ProcessingGroup]
        Processing groups using this photometric source

    Examples
    --------
    >>> aperture_2px = PhotometricSource(name="Aperture_2px",
    ...                                  description="2 pixel radius aperture")
    """

    __tablename__ = "photometric_source"

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)

    processing_groups: orm.Mapped[list["ProcessingGroup"]] = orm.relationship(
        "ProcessingGroup", back_populates="photometric_source"
    )


class DetrendingMethod(LCDBModel, NameAndDescriptionMixin):
    """
    Represents a method for removing systematic trends from lightcurves.

    DetrendingMethod defines algorithms used to remove instrumental or
    systematic effects from photometric time series, such as PDC-SAP
    (Pre-search Data Conditioning Simple Aperture Photometry).

    Attributes
    ----------
    id : int
        Primary key identifier
    name : str
        Name of the detrending method (inherited from mixin)
    description : str
        Detailed description (inherited from mixin)
    processing_groups : list[ProcessingGroup]
        Processing groups using this detrending method

    Examples
    --------
    >>> pdc = DetrendingMethod(name="PDC-SAP",
    ...                        description="Pre-search Data Conditioning")
    """

    __tablename__ = "detrending_method"

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    processing_groups: orm.Mapped[list["ProcessingGroup"]] = orm.relationship(
        "ProcessingGroup", back_populates="detrending_method"
    )


class ProcessingGroup(LCDBModel, NameAndDescriptionMixin):
    """
    Combines a photometric source with a detrending method.

    ProcessingGroup represents a unique combination of how photometry
    was extracted and how it was detrended. This allows tracking
    different processing pipelines applied to the same observations.

    Attributes
    ----------
    id : int
        Primary key identifier
    name : str
        Name of the processing group (inherited from mixin)
    description : str
        Detailed description (inherited from mixin)
    photometric_source_id : int
        Foreign key to photometric source
    detrending_method_id : int
        Foreign key to detrending method
    photometric_source : PhotometricSource
        The photometric extraction method
    detrending_method : DetrendingMethod
        The detrending algorithm
    interpretations : list[Interpretation]
        Lightcurve interpretations using this processing

    Notes
    -----
    The combination of photometric_source_id and detrending_method_id
    must be unique, ensuring no duplicate processing groups.
    """

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
    """
    A processed lightcurve for a specific target and observation.

    Interpretation is the central model that connects a target, an
    observation, and a processing method to produce a final lightcurve.
    It stores the actual photometric measurements and uncertainties.

    Attributes
    ----------
    id : int
        Primary key identifier
    processing_group_id : int
        Foreign key to processing group
    target_id : int
        Foreign key to target
    observation_id : int
        Foreign key to observation
    values : ndarray[float64]
        Array of photometric measurements (flux or magnitude)
    errors : ndarray[float64], optional
        Array of measurement uncertainties
    processing_group : ProcessingGroup
        The processing method used
    target : Target
        The astronomical target
    observation : Observation
        The source observation

    Notes
    -----
    This is the main table for storing lightcurve data. Each row
    represents one complete lightcurve for a target processed
    with a specific method.
    """

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
