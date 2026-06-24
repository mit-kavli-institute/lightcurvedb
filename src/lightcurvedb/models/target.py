import decimal
import uuid
from functools import lru_cache
from typing import TYPE_CHECKING

import sqlalchemy as sa
from sqlalchemy.ext import associationproxy as ap
from astropy import time
from astropy import units as u
from sqlalchemy import orm

from lightcurvedb.core.base_model import (
    CreatedOnMixin,
    LCDBModel,
    NameAndDescriptionMixin,
)

if TYPE_CHECKING:
    from lightcurvedb.models.dataset import DataSet
    from lightcurvedb.models.observation import TargetSpecificTime
    from lightcurvedb.models.quality_flag import QualityFlagArray


class Mission(LCDBModel, NameAndDescriptionMixin, CreatedOnMixin):
    """
    Represents a space mission or survey program.

    A Mission defines the top-level context for astronomical observations,
    including time system definitions and associated catalogs. Examples
    include TESS (Transiting Exoplanet Survey Satellite).

    Attributes
    ----------
    id : UUID
        Unique identifier for the mission
    name : str
        Unique name of the mission (e.g., "TESS")
    description : str
        Detailed description of the mission
    time_unit : str
        Unit of time measurement (e.g., "day")
    time_epoch : Decimal
        Reference epoch for time calculations
    time_epoch_scale : str
        Time scale for the epoch (e.g., "tdb")
    time_epoch_format : str
        Format of the epoch specification
    time_format_name : str
        Unique name for the mission's time format
    catalogs : list[MissionCatalog]
        Associated catalogs for this mission

    Examples
    --------
    >>> mission = Mission(name="TESS",
    ...                   description="Transiting Exoplanet Survey Satellite")
    """

    __tablename__ = "mission"
    __table_args__ = (sa.UniqueConstraint("name"),)

    id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        primary_key=True, default=uuid.uuid4
    )

    time_unit = orm.Mapped[str]
    time_epoch: orm.Mapped[decimal.Decimal] = orm.mapped_column()
    time_epoch_scale: orm.Mapped[str]
    time_epoch_format: orm.Mapped[str]
    time_format_name: orm.Mapped[str] = orm.mapped_column(unique=True)

    @lru_cache
    def register_mission_time_epoch(self):
        class MissionTime(time.TimeEpochDate):
            name = self.time_format_name
            unit = 1 * getattr(u, self.time_unit)
            epoch_val = self.time_epoch
            epoch_scale = self.time_epoch_scale
            epoch_format = self.time_epoch_format

        return MissionTime

    # Relationships
    catalogs: orm.Mapped[list["MissionCatalog"]] = orm.relationship(
        back_populates="host_mission"
    )

    def __repr__(self) -> str:
        return f"<Mission(id={self.id!s}, name={self.name!r})>"

    def __rich_repr__(self):
        yield "id", self.id
        yield "name", self.name


class MissionCatalog(LCDBModel, NameAndDescriptionMixin, CreatedOnMixin):
    """
    A catalog of astronomical targets associated with a mission.

    MissionCatalog represents a specific catalog within a mission context,
    such as the TESS Input Catalog (TIC). It serves as a container for
    organizing targets observed by the mission.

    Attributes
    ----------
    id : int
        Primary key identifier
    host_mission_id : UUID
        Foreign key to the parent Mission
    name : str
        Unique catalog name (e.g., "TIC" for TESS Input Catalog)
    description : str, optional
        Detailed description of the catalog
    host_mission : Mission
        Parent mission this catalog belongs to
    targets : list[Target]
        Collection of targets in this catalog
    """

    __tablename__ = "mission_catalog"
    __table_args__ = (sa.UniqueConstraint("host_mission_id", "name"),)

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    host_mission_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.ForeignKey(Mission.id, ondelete="CASCADE")
    )

    # Relationships
    host_mission: orm.Mapped["Mission"] = orm.relationship(
        back_populates="catalogs"
    )
    targets: orm.Mapped[list["Target"]] = orm.relationship(
        back_populates="catalog"
    )

    def __repr__(self) -> str:
        return (
            f"<MissionCatalog(id={self.id!r}, name={self.name!r}, "
            f"mission={self.host_mission_id!s})>"
        )

    def __rich_repr__(self):
        yield "id", self.id
        yield "name", self.name
        yield "mission", self.host_mission_id


class Target(LCDBModel):
    """
    An astronomical target (star, planet, etc.) in a mission catalog.

    Target represents an individual astronomical object that is observed
    during a mission. Each target is uniquely identified within its catalog
    by a numeric identifier (e.g., TIC ID for TESS targets).

    Attributes
    ----------
    id : int
        Primary key identifier
    catalog_id : int
        Foreign key to the MissionCatalog
    name : int
        Catalog-specific identifier (e.g., TIC ID)
    catalog : MissionCatalog
        The catalog this target belongs to
    datasets : list[DataSet]
        Processed lightcurve datasets for this target
    target_specific_times : list[TargetSpecificTime]
        Time series specific to this target
    quality_flag_arrays : list[QualityFlagArray]
        Target-specific quality flags
    parameters : list[AstroParameter]
        Astrophysical parameters measured for this target
    parameters_by_name : dict[str, AstroParameter]
        Read-only view of ``parameters`` keyed by parameter name

    Notes
    -----
    The combination of catalog_id and name must be unique,
    ensuring no duplicate targets within a catalog.

    Indexing a target by parameter name -- ``target["effective_temperature"]``
    -- returns that parameter as an astropy quantity (see
    :meth:`__getitem__`).
    """

    __tablename__ = "target"
    __table_args__ = (sa.UniqueConstraint("catalog_id", "name"),)

    id: orm.Mapped[int] = orm.mapped_column(sa.BigInteger, primary_key=True)
    catalog_id: orm.Mapped[int] = orm.mapped_column(
        sa.ForeignKey(
            MissionCatalog.id, ondelete="CASCADE", onupdate="CASCADE"
        )
    )
    name: orm.Mapped[int] = orm.mapped_column(sa.BigInteger)

    # Relationships
    catalog: orm.Mapped["MissionCatalog"] = orm.relationship(
        back_populates="targets"
    )
    # An alias pairing is symmetric and the two columns carry no order, so a
    # given target may occupy either one. These collections cover both
    # positions; use the ``aliases`` / ``aliased_targets`` properties below for
    # a position-agnostic view.
    _alias_links_as_target: orm.Mapped[list["Alias"]] = orm.relationship(
        foreign_keys="Alias.target_id",
        back_populates="target",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    _alias_links_as_counterpart: orm.Mapped[list["Alias"]] = orm.relationship(
        foreign_keys="Alias.counterpart_id",
        back_populates="counterpart",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    @property
    def aliases(self) -> list["Alias"]:
        """Every Alias row this target participates in, either position."""
        return [
            *self._alias_links_as_target,
            *self._alias_links_as_counterpart,
        ]

    @property
    def aliased_targets(self) -> list["Target"]:
        """The other target in each of this target's alias pairings."""
        return [link.counterpart for link in self._alias_links_as_target] + [
            link.target for link in self._alias_links_as_counterpart
        ]

    datasets: orm.Mapped[list["DataSet"]] = orm.relationship(
        back_populates="target"
    )
    target_specific_times: orm.Mapped[list["TargetSpecificTime"]] = (
        orm.relationship(
            back_populates="target",
            cascade="all, delete-orphan",
            passive_deletes=True,
        )
    )
    quality_flag_arrays: orm.Mapped[list["QualityFlagArray"]] = (
        orm.relationship(back_populates="target")
    )
    parameters: orm.Mapped[list["AstroParameter"]] = orm.relationship(
        back_populates="target",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    # Read-only dict view of the same rows, keyed by AstroParameter.name
    # (unique per target). viewonly so writes go through ``astro_parameters``;
    # keys are read from loaded rows, sidestepping the keyed-collection
    # transient-key pitfall.
    parameters_by_name: orm.Mapped[dict[str, "AstroParameter"]] = (
        orm.relationship(
            collection_class=orm.attribute_keyed_dict("name"),
            viewonly=True,
        )
    )

    def __repr__(self) -> str:
        return (
            f"<Target(id={self.id!r}, catalog={self.catalog_id!r}, "
            f"name={self.name!r})>"
        )

    def __rich_repr__(self):
        yield "id", self.id
        yield "catalog", self.catalog_id
        yield "name", self.name

    def __getitem__(self, key: str) -> u.Quantity:
        """
        Return a measured parameter as an astropy quantity by name.

        Enables ``target["effective_temperature"]``: looks the parameter up
        in :attr:`parameters_by_name` and returns
        :meth:`AstroParameter.as_quantity`.

        Parameters
        ----------
        key : str
            The parameter name (i.e. its unit's name).

        Returns
        -------
        astropy.units.Quantity
            ``value * unit`` for the named parameter.

        Raises
        ------
        KeyError
            If the target has no parameter with that name.
        """
        if key not in self.parameters_by_name:
            raise KeyError(f"{key!r} is not a parameter of {self!r}")
        return self.parameters_by_name[key].as_quantity()


class Alias(LCDBModel):
    """
    A symmetric cross-identification between two targets.

    Alias records that two catalog entries are believed to refer to the same
    astronomical object. Aliasing is most often a one-to-one match across
    catalogs (the same star with two catalog IDs), but it also captures the
    ambiguous cases: a single older entry that a modern catalog resolves into
    several distinct objects, or several entries later found to be one object.

    The relation is **symmetric** -- "A aliases B" is identical to "B aliases
    A" -- so each pairing is stored exactly once and the two columns carry no
    direction or ordering. It is deliberately **not transitive**: each row
    asserts only the single correspondence it names. In a split, target X may
    alias both Y and Z without implying Y aliases Z.

    Attributes
    ----------
    id : int
        Primary key identifier
    target_id : int
        Foreign key to one member of the pairing
    counterpart_id : int
        Foreign key to the other member of the pairing
    target : Target
        The target referenced by ``target_id``
    counterpart : Target
        The target referenced by ``counterpart_id``

    Notes
    -----
    The two columns are interchangeable; neither is privileged and their values
    must not be assumed to follow any catalog ordering. A self-reference
    ``CheckConstraint`` forbids a target aliasing itself, and a unique index
    over ``least(target_id, counterpart_id), greatest(...)`` collapses the two
    storable orderings of a pair to a single row -- least/greatest is only a
    deterministic dedup key, not a meaningful order.

    To enumerate a target's aliases regardless of column, prefer
    :attr:`Target.aliases` / :attr:`Target.aliased_targets`.
    """

    __tablename__ = "alias"
    __table_args__ = (
        sa.CheckConstraint(
            "target_id <> counterpart_id", name="alias_no_self_reference"
        ),
        # Treat (a, b) and (b, a) as the same alias by deduplicating on the
        # unordered pair. least()/greatest() canonicalize purely for the index
        # key and imply no ordering of the targets themselves. Expressed as
        # text() so the index can live inline without resolved Column objects.
        sa.Index(
            "uq_alias_unordered_pair",
            sa.text("least(target_id, counterpart_id)"),
            sa.text("greatest(target_id, counterpart_id)"),
            unique=True,
        ),
    )

    id: orm.Mapped[int] = orm.mapped_column(sa.BigInteger, primary_key=True)
    target_id: orm.Mapped[int] = orm.mapped_column(
        sa.ForeignKey(Target.id, ondelete="CASCADE", onupdate="CASCADE")
    )
    counterpart_id: orm.Mapped[int] = orm.mapped_column(
        sa.ForeignKey(Target.id, ondelete="CASCADE", onupdate="CASCADE")
    )

    # Relationships
    target: orm.Mapped["Target"] = orm.relationship(
        foreign_keys=[target_id],
        back_populates="_alias_links_as_target",
    )
    counterpart: orm.Mapped["Target"] = orm.relationship(
        foreign_keys=[counterpart_id],
        back_populates="_alias_links_as_counterpart",
    )

    @classmethod
    def between(cls, a: "Target", b: "Target") -> "Alias":
        """
        Build an alias pairing two targets, in either argument order.

        The pair is stored as given; the unique index treats ``(a, b)`` and
        ``(b, a)`` as the same row. Raises ``ValueError`` if the same target is
        passed twice, since a target cannot alias itself.
        """
        if a.id == b.id:
            raise ValueError("a target cannot be aliased to itself")
        return cls(target=a, counterpart=b)

    def __repr__(self) -> str:
        return (
            f"<Alias(id={self.id!r}, target={self.target_id!r}, "
            f"counterpart={self.counterpart_id!r})>"
        )

    def __rich_repr__(self):
        yield "id", self.id
        yield "target", self.target_id
        yield "counterpart", self.counterpart_id


class AstroUnit(LCDBModel):
    """
    A physical unit, stored as its astropy string representation.

    AstroUnit persists an :class:`astropy.units.UnitBase` by its generic
    string form (``unit_str``) and rebuilds the live unit on demand via
    :meth:`as_unit`. Storing the string keeps arbitrary named and composite
    units (``m``, ``mag``, ``erg / (cm2 s)``) representable without a fixed
    enumeration, while round-tripping exactly for the physically meaningful
    units used in practice.

    Attributes
    ----------
    id : int
        Primary key identifier.
    name : str
        Unique label identifying the quantity (e.g.
        ``"effective_temperature"``). Serves as the keyword for
        :attr:`AstroParameter.name` and ``Target.parameters_by_name``.
    unit_str : str
        The unit's astropy generic string form, e.g. ``"K"`` or ``"m / s"``.
    description : str
        Optional free-text description; defaults to an empty string.
    parameters : list[AstroParameter]
        Parameters expressed in this unit (shared lookup; not owned).

    Notes
    -----
    The reconstructed unit follows :class:`astropy.units.UnitBase` equality,
    which compares physical decomposition and scale. Extreme-magnitude
    composites can exceed float64 range during astropy's own decomposition;
    such units fall outside the intended scope.

    ``name`` is unique: each row defines one named quantity-kind (with
    ``unit_str`` giving that quantity's unit), which is how parameters are
    keyed on a target. Distinct kinds may share a ``unit_str`` (e.g. two
    temperatures both in ``"K"``).

    Examples
    --------
    >>> from astropy import units as u
    >>> unit = AstroUnit.reflect_astropy_unit(u.m / u.s, name="velocity")
    >>> unit.unit_str
    'm / s'
    >>> unit.as_unit() == u.m / u.s
    True
    """

    __tablename__ = "astro_unit"
    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    name: orm.Mapped[str] = orm.mapped_column(index=True, unique=True)
    unit_str: orm.Mapped[str]
    description: orm.Mapped[str] = orm.mapped_column(sa.TEXT, default="")

    # Relationships
    parameters: orm.Mapped[list["AstroParameter"]] = orm.relationship(
        lazy=True,
        back_populates="unit",
    )

    def as_unit(self):
        """
        Reconstruct the live astropy unit from ``unit_str``.

        Returns
        -------
        astropy.units.UnitBase
            The unit parsed from :attr:`unit_str` via
            :func:`astropy.units.Unit`.
        """
        return u.Unit(self.unit_str)

    @classmethod
    def reflect_astropy_unit(
        cls, astropy_unit_or_quantity: u.UnitBase | u.Quantity, **kwargs
    ) -> "AstroUnit":
        """
        Build an :class:`AstroUnit` from an astropy unit or quantity.

        The unit is serialized with ``str()``. For a quantity only its unit
        is stored; the scalar magnitude is discarded.

        Parameters
        ----------
        astropy_unit_or_quantity : UnitBase or Quantity
            An :class:`astropy.units.UnitBase` to reflect, or an
            :class:`astropy.units.Quantity` whose unit is reflected.
        **kwargs
            Extra column values forwarded to the constructor, e.g. ``name``
            (required; ``NOT NULL``) and ``description``.

        Returns
        -------
        AstroUnit
            An unsaved instance with ``unit_str`` set from the input.

        Raises
        ------
        NotImplementedError
            If the argument is neither a unit nor a quantity.

        Notes
        -----
        Matching is on :class:`astropy.units.UnitBase`, not
        :class:`astropy.units.Unit`: irreducible units (``u.m``) and composite
        units (``u.m / u.s``) are ``UnitBase`` subclasses but not ``Unit``
        instances, so matching ``Unit`` would reject all but prefixed units.

        Examples
        --------
        >>> from astropy import units as u
        >>> AstroUnit.reflect_astropy_unit(u.K, name="temp").unit_str
        'K'
        """
        match astropy_unit_or_quantity:
            case u.UnitBase():
                return cls(unit_str=str(astropy_unit_or_quantity), **kwargs)
            case u.Quantity():
                unit = astropy_unit_or_quantity.unit
                return cls(unit_str=str(unit), **kwargs)
            case _:
                raise NotImplementedError

    def __repr__(self) -> str:
        return (
            f"<AstroUnit(id={self.id!r}, name={self.name!r}, "
            f"unit_str={self.unit_str!r})>"
        )

    def __rich_repr__(self):
        yield "id", self.id
        yield "name", self.name
        yield "unit_str", self.unit_str


class AstroParameter(LCDBModel):
    """
    A measured astrophysical quantity for a target, with asymmetric errors.

    AstroParameter stores a scalar ``value`` alongside independent upper and
    lower uncertainties and a reference to the :class:`AstroUnit` the value is
    expressed in. The split errors capture the common ``value (+upper,
    -lower)`` reporting convention used in the literature.

    Attributes
    ----------
    id : int
        Primary key identifier.
    name : str
        Read-only. The parameter kind, mirrored from ``unit.name`` (e.g.
        ``"effective_temperature"``). Assign the kind on the
        :class:`AstroUnit`, not here.
    value : float
        The parameter value, expressed in the linked unit.
    upper_error : float
        Upper (positive-direction) uncertainty on ``value``.
    lower_error : float
        Lower (negative-direction) uncertainty on ``value``.
    target_id : int
        Foreign key to the :class:`Target` this parameter describes.
    unit_id : int
        Foreign key to the :class:`AstroUnit` giving the value's unit.
    target : Target
        The target this parameter describes.
    unit : AstroUnit
        The unit the value is expressed in.

    Notes
    -----
    Keeping ``value`` consistent with its unit is the caller's
    responsibility: the model stores and returns both verbatim and performs
    no unit conversion or scale normalization.

    The unique constraint on ``(target_id, unit_id)`` permits one parameter
    per unit on a given target. Because each :class:`AstroUnit` has a unique
    ``name``, that is equivalently one parameter per name -- so ``name``
    identifies a parameter within its target.
    """

    __tablename__ = "astro_parameter"
    __table_args__ = (
        sa.UniqueConstraint(
            "target_id",
            "unit_id",
        ),
    )

    id: orm.Mapped[int] = orm.mapped_column(sa.BigInteger, primary_key=True)
    # Read-only view of the parameter kind; mirrors unit.name. Assign the kind
    # on the AstroUnit -- writing here would rename the shared unit.
    name: ap.AssociationProxy[str] = ap.association_proxy("unit", "name")
    value: orm.Mapped[float]
    upper_error: orm.Mapped[float]
    lower_error: orm.Mapped[float]
    target_id: orm.Mapped[int] = orm.mapped_column(
        sa.ForeignKey(Target.id, ondelete="CASCADE", onupdate="CASCADE"),
        index=True,
    )
    unit_id: orm.Mapped[int] = orm.mapped_column(
        sa.ForeignKey(AstroUnit.id, ondelete="RESTRICT"),
        index=True,
    )

    # Relationships
    target: orm.Mapped["Target"] = orm.relationship(
        back_populates="parameters",
    )
    unit: orm.Mapped["AstroUnit"] = orm.relationship(
        back_populates="parameters",
    )

    def as_quantity(self) -> u.Quantity:
        """
        Combine ``value`` with its unit into an astropy quantity.

        Returns
        -------
        astropy.units.Quantity
            ``self.value * self.unit.as_unit()``, using ``value`` verbatim
            with no scale conversion.

        Notes
        -----
        Requires the :attr:`unit` relationship; an attached instance
        lazy-loads it. Raises ``AttributeError`` if :attr:`unit` is ``None``.
        """
        return self.value * self.unit.as_unit()

    def __repr__(self) -> str:
        return (
            f"<AstroParameter(id={self.id!r}, name={self.name!r}, "
            f"value={self.value!r}, target={self.target_id!r}, "
            f"unit={self.unit_id!r})>"
        )

    def __rich_repr__(self):
        yield "id", self.id
        yield "name", self.name
        yield "value", self.value
        yield "target", self.target_id
        yield "unit", self.unit_id
