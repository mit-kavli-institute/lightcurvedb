"""Association tables for many-to-many relationships."""

import uuid

import sqlalchemy as sa
from sqlalchemy import orm

from lightcurvedb.core.base_model import LCDBModel


class ObservationFITSFrameAssociation(LCDBModel):
    """Association for Observation and FITSFrame many-to-many relationship."""

    __tablename__ = "observation_fits_frame"

    observation_id: orm.Mapped[int] = orm.mapped_column(
        sa.ForeignKey("observation.id", ondelete="CASCADE"), primary_key=True
    )
    fits_frame_id: orm.Mapped[int] = orm.mapped_column(
        sa.ForeignKey("fits_frame.id", ondelete="CASCADE"), primary_key=True
    )


class ObservationInstrumentAssociation(LCDBModel):
    """Association for Observation and Instrument many-to-many relationship."""

    __tablename__ = "observation_instrument"

    observation_id: orm.Mapped[int] = orm.mapped_column(
        sa.ForeignKey("observation.id", ondelete="CASCADE"), primary_key=True
    )
    instrument_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.ForeignKey("instrument.id", ondelete="CASCADE"), primary_key=True
    )
