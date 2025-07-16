import decimal
import typing
import uuid
from functools import lru_cache
from typing import TYPE_CHECKING

import sqlalchemy as sa
from astropy import time
from astropy import units as u
from sqlalchemy import orm

from lightcurvedb.core.base_model import LCDBModel

if TYPE_CHECKING:
    from lightcurvedb.models.interpretation import Interpretation
    from lightcurvedb.models.observation import TargetSpecificTime


class Mission(LCDBModel):
    __tablename__ = "mission"
    id: orm.Mapped[uuid.UUID] = orm.mapped_column(primary_key=True)
    name: orm.Mapped[str] = orm.mapped_column(unique=True)
    description: orm.Mapped[str]

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


class MissionCatalog(LCDBModel):
    __tablename__ = "mission_catalog"

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    host_mission_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.ForeignKey(Mission.id)
    )
    name: orm.Mapped[str] = orm.mapped_column(unique=True)
    description: orm.Mapped[typing.Optional[str]]

    # Relationships
    host_mission: orm.Mapped["Mission"] = orm.relationship(
        back_populates="catalogs"
    )
    targets: orm.Mapped[list["Target"]] = orm.relationship(
        back_populates="catalog"
    )


class Target(LCDBModel):
    __tablename__ = "target"
    __table_args__ = (sa.UniqueConstraint("catalog_id", "name"),)

    id: orm.Mapped[int] = orm.mapped_column(sa.BigInteger, primary_key=True)
    catalog_id: orm.Mapped[int] = orm.mapped_column(
        sa.ForeignKey(MissionCatalog.id)
    )
    name: orm.Mapped[int] = orm.mapped_column(sa.BigInteger)

    # Relationships
    catalog: orm.Mapped["MissionCatalog"] = orm.relationship(
        back_populates="targets"
    )
    interpretations: orm.Mapped[list["Interpretation"]] = orm.relationship(
        back_populates="target"
    )
    target_specific_times: orm.Mapped[
        list["TargetSpecificTime"]
    ] = orm.relationship(back_populates="target")
