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
    datasets : list[DataSet]
        Datasets using this photometric source

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
    target_id : int
        Foreign key to target
    observation_id : int
        Foreign key to observation
    photometric_method_id : int, optional
        Foreign key to photometric source. Nullable to allow datasets
        without explicit photometry method (e.g., derived products).
    processing_method_id : int, optional
        Foreign key to processing method. Nullable to allow raw photometry
        datasets without processing applied.
    values : ndarray[float64]
        Array of photometric measurements (flux or magnitude)
    errors : ndarray[float64], optional
        Array of measurement uncertainties
    target : Target
        The astronomical target
    observation : Observation
        The source observation
    photometry_source : PhotometricSource
        The photometric extraction method used
    processing_method : ProcessingMethod
        The processing operation applied
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

    def align_to_observation(
        self,
        dataset_cadences: npt.NDArray[np.integer],
        fill_value: float = np.nan,
    ) -> None:
        """
        Align dataset values and errors to the observation's cadence reference.

        Reindexes the dataset's ``values`` and ``errors`` arrays to match the
        observation's ``cadence_reference`` grid. Positions in the reference
        grid that have no corresponding data in ``dataset_cadences`` are filled
        with ``fill_value``.

        This method modifies the dataset in place.

        Parameters
        ----------
        dataset_cadences : ndarray of int
            Monotonically increasing cadence indices corresponding to the
            current ``values`` and ``errors`` arrays. Must be a subset of
            the observation's ``cadence_reference``.
        fill_value : float, optional
            Value to use for cadences in the reference grid that are not
            present in ``dataset_cadences``. Default is ``np.nan``.

        Raises
        ------
        ValueError
            If the dataset has no associated observation (``observation`` is
            None) or if ``values`` is None.

        See Also
        --------
        Observation.align_to_reference : The underlying alignment method.

        Notes
        -----
        This is useful when dataset values were extracted from a subset of
        frames in an observation and need to be expanded to match the full
        observation cadence grid for consistent array indexing across multiple
        datasets.

        The alignment preserves the original values at their correct positions
        and inserts ``fill_value`` at gaps. Both ``values`` and ``errors``
        (if present) are aligned using the same cadence mapping.

        Examples
        --------
        >>> import numpy as np
        >>> from lightcurvedb.models import DataSet, Observation

        Create an observation with a reference cadence grid:

        >>> obs = Observation(cadence_reference=np.array([1, 2, 3, 4, 5]))

        Create a dataset with values at only some cadences:

        >>> dataset = DataSet(
        ...     values=np.array([100.0, 200.0]),
        ...     errors=np.array([0.1, 0.2]),
        ...     observation=obs,
        ... )

        Align the dataset to the full observation grid:

        >>> dataset.align_to_observation(
        ...     dataset_cadences=np.array([2, 4]),
        ...     fill_value=-999.0,
        ... )
        >>> dataset.values
        array([-999.,  100., -999.,  200., -999.])
        >>> dataset.errors
        array([-999. ,    0.1, -999. ,    0.2, -999. ])
        """
        if self.observation is None:
            raise ValueError("Cannot align dataset: no observation associated")
        if self.values is None:
            raise ValueError("Cannot align dataset: values array is None")

        self.values = self.observation.align_to_reference(
            dataset_cadences,
            self.values,
            fill_value=fill_value,
        )

        if self.errors is not None:
            self.errors = self.observation.align_to_reference(
                dataset_cadences,
                self.errors,
                fill_value=fill_value,
            )
