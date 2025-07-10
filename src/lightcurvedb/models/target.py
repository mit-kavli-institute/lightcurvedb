import decimal
import typing
import uuid
from functools import lru_cache

import sqlalchemy as sa
from astropy import time
from astropy import units as u
from sqlalchemy import orm

from lightcurvedb.core.base_model import LCDBModel


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


class MissionCatalog(LCDBModel):
    __tablename__ = "mission_catalog"

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    host_mission_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.ForeignKey(Mission.id)
    )
    name: orm.Mapped[str] = orm.mapped_column(unique=True)
    description: orm.Mapped[typing.Optional[str]]


class Target(LCDBModel):
    __tablename__ = "target"
    __table_args__ = (sa.UniqueConstraint("catalog_id", "name"),)

    id: orm.Mapped[int] = orm.mapped_column(sa.BigInteger, primary_key=True)
    catalog_id: orm.Mapped[int] = orm.mapped_column(
        sa.ForeignKey(MissionCatalog.id)
    )
    name: orm.Mapped[int] = orm.mapped_column(sa.BigInteger)
