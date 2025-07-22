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
    datasets: orm.Mapped[list["DataSet"]] = orm.relationship(
        back_populates="target"
    )
    target_specific_times: orm.Mapped[
        list["TargetSpecificTime"]
    ] = orm.relationship(back_populates="target")
    quality_flag_arrays: orm.Mapped[
        list["QualityFlagArray"]
    ] = orm.relationship(back_populates="target")
