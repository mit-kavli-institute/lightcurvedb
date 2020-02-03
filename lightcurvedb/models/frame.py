from sqlalchemy import Column, ForeignKey, Integer, SmallInteger, BigInteger, String, Boolean
from sqlalchemy.orm import relationship
from lightcurvedb.core.base_model import QLPModel, QLPDataProduct, QLPDataSubType
from lightcurvedb.core.fields import high_precision_column


class FrameType(QLPDataSubType):
    """Describes the numerous frame types"""
    __tablename__ = 'frametypes'

    frames = relationship('Frame', back_populates='frame_type')


class Frame(QLPDataProduct):
    """
        Provides ORM implementation of various Frame models
    """

    __tablename__ = 'frames'

    # Model attributes
    cadence_type = Column(SmallInteger, index=True, nullable=False)
    camera = Column(SmallInteger, index=True, nullable=False)
    ccd = Column(SmallInteger, index=True, nullable=True)
    cadence = Column(Integer, index=True, nullable=False)

    gps_time = high_precision_column(nullable=False)
    start_tjd = high_precision_column(nullable=False)
    mid_tjd = high_precision_column(nullable=False)
    end_tjd = high_precision_column(nullable=False)
    exp_time = high_precision_column(nullable=False)

    quality_bit = Column(Boolean, nullable=False)

    file_path = Column(String, nullable=False)

    # Foreign Keys
    orbit_id = Column(Integer, ForeignKey('orbits.id', ondelete='RESTRICT'))
    frame_type_id = Column(ForeignKey('frametypes.id', ondelete='RESTRICT'))

    # Relationships
    orbit = relationship('Orbit', back_populates='frames')
    frame_type = relationship('FrameType', back_populates='frames')
