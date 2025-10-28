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

    datasets: orm.Mapped[list["DataSet"]] = orm.relationship(lazy=True)


class ProcessingMethod(LCDBModel, NameAndDescriptionMixin):
    """
    Represents a method for performing some non-pure, one-to-one
    function on a dataset.

    These modules should describe some complete unit of work/operation
    performed on a dataset such detrending or 'original' sources of data.

    Attributes
    ----------
    id : int
        Primary key identifier
    name : str
        Name of the detrending method (inherited from mixin)
    description : str
        Detailed description (inherited from mixin)
    Examples
    --------
    >>> pdc = ProcessingMethod(name="PDC-SAP",
    ...                        description="Pre-search Data Conditioning")
    """

    __tablename__ = "processing_method"

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)

    datasets: orm.Mapped[list["DataSet"]] = orm.relationship(lazy=True)


class DataSetHierarchy(LCDBModel):
    """
    Association table for creating hierarchical relationships between
    DataSets.

    This table enables a tree structure where datasets can be derived from
    other datasets, allowing tracking of data processing lineage and
    provenance. For example, a detrended lightcurve dataset might be
    derived from a raw photometry dataset.

    Attributes
    ----------
    source_dataset_id : int
        Foreign key to the parent/source dataset
    child_dataset_id : int
        Foreign key to the child/derived dataset

    Notes
    -----
    This is an association table for a many-to-many self-referential
    relationship. A dataset can have multiple parents (e.g., combining
    data from multiple sources) and multiple children (e.g., different
    processing methods applied to the same source).
    """

    __tablename__ = "datasethierarchy"

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    source_dataset_id: orm.Mapped[int] = orm.mapped_column(
        sa.ForeignKey("dataset.id", ondelete="CASCADE"), index=True
    )
    child_dataset_id: orm.Mapped[int] = orm.mapped_column(
        sa.ForeignKey("dataset.id", ondelete="CASCADE"), index=True
    )


class DataSet(LCDBModel):
    """
    A processed lightcurve for a specific target and observation.

    DataSet is the central model that connects a target, an
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
    target : Target
        The astronomical target
    observation : Observation
        The source observation
    source_datasets : list[DataSet]
        Parent datasets that this dataset was derived from. Enables tracking
        of data lineage and processing provenance.
    derived_datasets : list[DataSet]
        Child datasets that were derived from this dataset. Allows viewing
        all downstream processing results.

    Notes
    -----
    This is the main table for storing lightcurve data. Each row
    represents one complete lightcurve for a target processed
    with a specific method.

    The hierarchical relationships (source_datasets, derived_datasets) enable
    tracking data processing lineage. For example, a detrended lightcurve
    would list the raw photometry dataset in source_datasets, while the raw
    dataset would list all derived products in derived_datasets.

    Examples
    --------
    >>> # Create a hierarchical relationship
    >>> raw_dataset = DataSet(target=target, observation=obs, values=raw_flux)
    >>> detrended_dataset = DataSet(target=target, observation=obs,
    ...                             values=detrended_flux)
    >>> detrended_dataset.source_datasets.append(raw_dataset)
    >>> session.add_all([raw_dataset, detrended_dataset])
    >>> session.commit()
    """

    __tablename__ = "dataset"

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    target_id: orm.Mapped[int] = orm.mapped_column(
        sa.ForeignKey("target.id", ondelete="CASCADE"), index=True
    )
    observation_id: orm.Mapped[int] = orm.mapped_column(
        sa.ForeignKey("observation.id", ondelete="CASCADE"), index=True
    )
    photometric_method_id: orm.Mapped[int] = orm.mapped_column(
        sa.ForeignKey("photometric_source.id", ondelete="CASCADE"),
        index=True,
        nullable=True,
    )
    processing_method_id: orm.Mapped[int] = orm.mapped_column(
        sa.ForeignKey("processing_method.id", ondelete="CASCADE"),
        index=True,
        nullable=True,
    )

    values: orm.Mapped[npt.NDArray[np.float64]]
    errors: orm.Mapped[typing.Optional[npt.NDArray[np.float64]]]

    # Relationships
    target: orm.Mapped["Target"] = orm.relationship(back_populates="datasets")
    observation: orm.Mapped["Observation"] = orm.relationship(
        back_populates="datasets"
    )
    photometry_source: orm.Mapped["PhotometricSource"] = orm.relationship(
        back_populates="datasets"
    )
    processing_method: orm.Mapped["ProcessingMethod"] = orm.relationship(
        back_populates="datasets"
    )

    # Self-referential hierarchical relationships
    source_datasets: orm.Mapped[list["DataSet"]] = orm.relationship(
        "DataSet",
        secondary="datasethierarchy",
        primaryjoin="DataSet.id == DataSetHierarchy.child_dataset_id",
        secondaryjoin="DataSet.id == DataSetHierarchy.source_dataset_id",
        back_populates="derived_datasets",
    )

    derived_datasets: orm.Mapped[list["DataSet"]] = orm.relationship(
        "DataSet",
        secondary="datasethierarchy",
        primaryjoin="DataSet.id == DataSetHierarchy.source_dataset_id",
        secondaryjoin="DataSet.id == DataSetHierarchy.child_dataset_id",
        back_populates="source_datasets",
    )
