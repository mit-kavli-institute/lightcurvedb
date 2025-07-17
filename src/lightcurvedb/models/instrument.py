import typing
import uuid
from typing import TYPE_CHECKING

import sqlalchemy as sa
from sqlalchemy import orm

from lightcurvedb.core.base_model import LCDBModel

if TYPE_CHECKING:
    from lightcurvedb.models.observation import Observation


class Instrument(LCDBModel):
    """
    A class to represent hardware on scientific instrument/assembly.
    Instruments can be literal observation equipment (such as a CCD) or be
    an assembly of other instruments (i.e a camera housing multiple CCDs).
    """

    __tablename__ = "instrument"

    id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        primary_key=True, default=uuid.uuid4
    )
    name: orm.Mapped[str]
    properties: orm.Mapped[dict[str, typing.Any]]

    parent_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.ForeignKey("instrument.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    parent: orm.Mapped["Instrument"] = orm.relationship(
        "Instrument", back_populates="children", remote_side=[id]
    )
    children: orm.Mapped[list["Instrument"]] = orm.relationship(
        "Instrument", back_populates="parent"
    )

    # Related observations
    observations: orm.Mapped[list["Observation"]] = orm.relationship(
        back_populates="instrument",
    )

    @classmethod
    def query_for_instrument(
        cls, name: str, parent_name: typing.Optional[str] = None
    ):
        q = sa.select(cls).where(cls.name == name)
        if parent_name:
            parent = orm.aliased(cls)
            q = q.join(cls.parent.of_type(parent)).where(
                parent.name == parent_name
            )
        else:
            q = q.where(cls.parent_id.is_(None))
        return q
