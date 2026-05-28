import decimal
import uuid
from functools import lru_cache
from typing import TYPE_CHECKING

import sqlalchemy as sa
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

    Notes
    -----
    The combination of catalog_id and name must be unique,
    ensuring no duplicate targets within a catalog.
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
    target_specific_times: orm.Mapped[
        list["TargetSpecificTime"]
    ] = orm.relationship(
        back_populates="target",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    quality_flag_arrays: orm.Mapped[
        list["QualityFlagArray"]
    ] = orm.relationship(back_populates="target")

    def __repr__(self) -> str:
        return (
            f"<Target(id={self.id!r}, catalog={self.catalog_id!r}, "
            f"name={self.name!r})>"
        )

    def __rich_repr__(self):
        yield "id", self.id
        yield "catalog", self.catalog_id
        yield "name", self.name


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
