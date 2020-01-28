from sqlalchemy import Column, ForeignKey, Integer, String, BigInteger, Float
from sqlalchemy.orm import relationship
from lightcurvedb.core.base_model import QLPReference
from lightcurvedb.core.fields import high_precision_column


class Orbit(QLPReference):
    """
        Provides ORM implementation of an orbit completed by TESS
    """

    __tablename__ = 'orbits'

    # Model Attributes
    orbit_number = Column(Integer, unique=True, nullable=False)
    sector = Column(Integer, nullable=False)

    right_ascension = high_precision_column(nullable=False)
    declination = high_precision_column(nullable=False)
    roll = high_precision_column(nullable=False)

    quaternion_x = high_precision_column(nullable=False)
    quaternion_y = high_precision_column(nullable=False)
    quaternion_z = high_precision_column(nullable=False)
    quaternion_q = high_precision_column(nullable=False)

    crm_n = Column(Integer, nullable=False)  # Cosmic Ray Mitigation Number
    basename = Column(String(256), nullable=False)

    # Relationships
    frames = relationship('Frame', back_populates='orbit')
    lightcurves = relationship('OrbitLightcurve', back_populates='orbit')
