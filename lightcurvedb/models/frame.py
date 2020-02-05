from sqlalchemy import Column, ForeignKey, Integer, SmallInteger, BigInteger, String, Boolean
from sqlalchemy.orm import relationship
from sqlalchemy.schema import UniqueConstraint, CheckConstraint
from lightcurvedb.core.base_model import QLPModel, QLPDataProduct, QLPDataSubType
from lightcurvedb.core.fields import high_precision_column


class FrameType(QLPDataSubType):
    """Describes the numerous frame types"""
    __tablename__ = 'frametypes'

    frames = relationship('Frame', back_populates='frame_type')

    def __repr__(self):
        return 'FrameType(name="{}", description="{}")'.format(self.name, self.description)


class Frame(QLPDataProduct):
    """
        Provides ORM implementation of various Frame models
    """

    __tablename__ = 'frames'

    # Constraints
    __table_args__ = (
        UniqueConstraint(
            'frame_type_id',
            'orbit_id',
            'cadence',
            'camera',
            'ccd',
            name='unique_frame'),
        CheckConstraint('camera BETWEEN 1 and 4', name='physical_camera_constraint'),
        CheckConstraint('(ccd IS NULL) OR (ccd BETWEEN 1 AND 4)', name='physical_ccd_constraint'),
    )

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

    file_path = Column(String, nullable=False, unique=True)

    # Foreign Keys
    orbit_id = Column(Integer, ForeignKey('orbits.id', ondelete='RESTRICT'))
    frame_type_id = Column(ForeignKey('frametypes.id', ondelete='RESTRICT'))

    # Relationships
    orbit = relationship('Orbit', back_populates='frames')
    frame_type = relationship('FrameType', back_populates='frames')
