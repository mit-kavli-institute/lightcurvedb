import typing
from typing import TYPE_CHECKING, ClassVar

import numpy as np
import sqlalchemy as sa
from numpy import typing as npt
from sqlalchemy import orm
from sqlalchemy.ext.hybrid import hybrid_property

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

    The sentinel record with id=0 represents "unspecified" photometric source
    for datasets where the source is not applicable or unknown.

    Attributes
    ----------
    id : int
        Primary key identifier (0 = unspecified sentinel)
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

    UNSPECIFIED_ID: ClassVar[int] = 0

    id: orm.Mapped[int] = orm.mapped_column(
        primary_key=True,
        autoincrement=False,
    )

    datasets: orm.Mapped[list["DataSet"]] = orm.relationship(
        lazy=True,
        back_populates="photometry_source",
    )

    @classmethod
    def get_or_create_unspecified(
        cls, session: orm.Session
    ) -> "PhotometricSource":
        """
        Get or create the unspecified sentinel record.

        Parameters
        ----------
        session : orm.Session
            Active database session.

        Returns
        -------
        PhotometricSource
            The sentinel record with id=0.
        """
        sentinel = session.get(cls, cls.UNSPECIFIED_ID)
        if sentinel is None:
            sentinel = cls(
                id=cls.UNSPECIFIED_ID,
                name="Unspecified",
                description="No photometric source specified",
            )
            session.add(sentinel)
            session.flush()
        return sentinel

    def __repr__(self) -> str:
        return f"<PhotometricSource(id={self.id!r}, name={self.name!r})>"

    def __rich_repr__(self):
        yield "id", self.id
        yield "name", self.name


class ProcessingMethod(LCDBModel, NameAndDescriptionMixin):
    """
    Represents a method for performing some non-pure, one-to-one
    function on a dataset.

    These modules should describe some complete unit of work/operation
    performed on a dataset such detrending or 'original' sources of data.

    The sentinel record with id=0 represents "unspecified" processing method
    for raw datasets where no processing has been applied.

    Attributes
    ----------
    id : int
        Primary key identifier (0 = unspecified sentinel)
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

    UNSPECIFIED_ID: ClassVar[int] = 0

    id: orm.Mapped[int] = orm.mapped_column(
        primary_key=True,
        autoincrement=False,
    )

    datasets: orm.Mapped[list["DataSet"]] = orm.relationship(
        lazy=True,
        back_populates="processing_method",
    )

    @classmethod
    def get_or_create_unspecified(
        cls, session: orm.Session
    ) -> "ProcessingMethod":
        """
        Get or create the unspecified sentinel record.

        Parameters
        ----------
        session : orm.Session
            Active database session.

        Returns
        -------
        ProcessingMethod
            The sentinel record with id=0.
        """
        sentinel = session.get(cls, cls.UNSPECIFIED_ID)
        if sentinel is None:
            sentinel = cls(
                id=cls.UNSPECIFIED_ID,
                name="Unspecified",
                description="No processing method applied (raw data)",
            )
            session.add(sentinel)
            session.flush()
        return sentinel

    def __repr__(self) -> str:
        return f"<ProcessingMethod(id={self.id!r}, name={self.name!r})>"

    def __rich_repr__(self):
        yield "id", self.id
        yield "name", self.name


class DataSet(LCDBModel):
    """
    A processed lightcurve for a specific target and observation.

    DataSet is the central model that connects a target, an observation,
    and a processing method to produce a final lightcurve. It stores the
    actual photometric measurements and uncertainties.

    The composite primary key (observation_id, target_id,
    photometric_method_id, processing_method_id) uniquely identifies each
    dataset. The table is
    partitioned by observation_id using PostgreSQL LIST partitioning for
    efficient query performance at scale.

    Attributes
    ----------
    observation_id : int
        Foreign key to observation (part of composite PK, partition key)
    target_id : int
        Foreign key to target (part of composite PK)
    photometric_method_id : int
        Foreign key to photometric source (part of composite PK).
        Use PhotometricSource.UNSPECIFIED_ID (0) for unspecified.
    processing_method_id : int
        Foreign key to processing method (part of composite PK).
        Use ProcessingMethod.UNSPECIFIED_ID (0) for unspecified.
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

    Notes
    -----
    This is the main table for storing lightcurve data. Each row represents
    one complete lightcurve for a target processed with a specific method.

    The table uses PostgreSQL LIST partitioning by observation_id, with each
    observation containing millions of rows as a natural partition boundary.
    Partitions are managed by database administrators.

    Examples
    --------
    >>> dataset = DataSet(
    ...     target=target,
    ...     observation=obs,
    ...     values=flux,
    ...     photometric_method_id=source.id,
    ...     processing_method_id=ProcessingMethod.UNSPECIFIED_ID,
    ... )
    >>> session.add(dataset)
    >>> session.commit()
    """

    __tablename__ = "dataset"

    __table_args__ = (
        sa.PrimaryKeyConstraint(
            "observation_id",
            "target_id",
            "photometric_method_id",
            "processing_method_id",
            name="pk_dataset",
        ),
        {"postgresql_partition_by": "LIST (observation_id)"},
    )

    # Composite PK columns (observation_id first for partitioning)
    observation_id: orm.Mapped[int] = orm.mapped_column(
        sa.ForeignKey("observation.id", ondelete="CASCADE"),
        nullable=False,
    )
    target_id: orm.Mapped[int] = orm.mapped_column(
        sa.ForeignKey("target.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    photometric_method_id: orm.Mapped[int] = orm.mapped_column(
        sa.ForeignKey("photometric_source.id", ondelete="RESTRICT"),
        nullable=False,
        default=0,
    )
    processing_method_id: orm.Mapped[int] = orm.mapped_column(
        sa.ForeignKey("processing_method.id", ondelete="RESTRICT"),
        nullable=False,
        default=0,
    )

    # Data columns
    values: orm.Mapped[npt.NDArray[np.float64]]
    errors: orm.Mapped[typing.Optional[npt.NDArray[np.float64]]]

    # Relationships
    target: orm.Mapped["Target"] = orm.relationship(back_populates="datasets")
    observation: orm.Mapped["Observation"] = orm.relationship(
        back_populates="datasets"
    )
    photometry_source: orm.Mapped["PhotometricSource"] = orm.relationship(
        back_populates="datasets",
        foreign_keys=[photometric_method_id],
    )
    processing_method: orm.Mapped["ProcessingMethod"] = orm.relationship(
        back_populates="datasets",
        foreign_keys=[processing_method_id],
    )

    @hybrid_property
    def has_photometric_source(self) -> bool:
        """Return True if a specific photometric source is set."""
        return self.photometric_method_id != PhotometricSource.UNSPECIFIED_ID

    @has_photometric_source.expression
    def has_photometric_source(cls):
        """SQL expression for filtering datasets with photometric source."""
        return cls.photometric_method_id != PhotometricSource.UNSPECIFIED_ID

    @hybrid_property
    def has_processing_method(self) -> bool:
        """Return True if a specific processing method is set."""
        return self.processing_method_id != ProcessingMethod.UNSPECIFIED_ID

    @has_processing_method.expression
    def has_processing_method(cls):
        """SQL expression for filtering datasets with processing method."""
        return cls.processing_method_id != ProcessingMethod.UNSPECIFIED_ID

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

    def __repr__(self) -> str:
        return (
            f"<DataSet(obs={self.observation_id}, target={self.target_id}, "
            f"phot={self.photometric_method_id}, "
            f"proc={self.processing_method_id})>"
        )

    def __rich_repr__(self):
        yield "obs", self.observation_id
        yield "target", self.target_id
        yield "phot", self.photometric_method_id
        yield "proc", self.processing_method_id
