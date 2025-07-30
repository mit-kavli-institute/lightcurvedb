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
    Represents a scientific instrument or assembly used for observations.

    Instruments form a hierarchical structure where an instrument can be
    either a physical device (e.g., a CCD) or an assembly containing other
    instruments (e.g., a camera with multiple CCDs). This allows modeling
    complex instrument configurations.

    Attributes
    ----------
    id : UUID
        Unique identifier for the instrument
    name : str
        Name of the instrument (e.g., "Camera 1", "CCD 2")
    properties : dict
        JSON dictionary of instrument-specific properties and metadata
    parent_id : UUID, optional
        Foreign key to parent instrument (None for top-level instruments)
    parent : Instrument, optional
        Parent instrument in the hierarchy
    children : list[Instrument]
        Child instruments if this is an assembly
    observations : list[Observation]
        Observations made using this instrument

    Examples
    --------
    >>> camera = Instrument(name="TESS Camera 1")
    >>> ccd = Instrument(name="CCD 1", parent=camera)

    Notes
    -----
    The self-referential relationship allows building instrument trees
    of arbitrary depth, useful for complex telescope configurations.
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
