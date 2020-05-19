from sqlalchemy import Column, ForeignKey, Integer, String, BigInteger, Float, Boolean, Sequence
from sqlalchemy.orm import relationship
from lightcurvedb.core.base_model import QLPReference
from lightcurvedb.core.fields import high_precision_column


class Orbit(QLPReference):
    """
        Provides ORM implementation of an orbit completed by TESS
    """

    __tablename__ = 'orbits'

    # Model Attributes
    id = Column(Integer, Sequence('orbit_id_seq'), primary_key=True)
    orbit_number = Column(Integer, unique=True, nullable=False)
    sector = Column(Integer, nullable=False)

    right_ascension = high_precision_column(nullable=False)
    declination = high_precision_column(nullable=False)
    roll = high_precision_column(nullable=False)

    quaternion_x = high_precision_column(nullable=False)
    quaternion_y = high_precision_column(nullable=False)
    quaternion_z = high_precision_column(nullable=False)
    quaternion_q = high_precision_column(nullable=False)

    crm = Column(Boolean, nullable=False) # Has been correct for CRM
    crm_n = Column(Integer, nullable=False)  # Cosmic Ray Mitigation Number
    basename = Column(String(256), nullable=False)

    # Relationships
    frames = relationship('Frame', back_populates='orbit')
    observations = relationship('Observation', back_populates='orbit')

    def __repr__(self):
        return 'Orbit-{} Sector-{} ({:.3f}, {:.3f}, {:.3f}) {}'.format(
            self.orbit_number,
            self.sector,
            self.right_ascension,
            self.declination,
            self.roll,
            self.basename
        )

    def copy_from(self, other_orbit):
        # load in the other attributes
        self.orbit_number = other_orbit.orbit_number
        self.sector = other_orbit.sector
        self.right_ascension = other_orbit.right_ascension
        self.declination = other_orbit.declination
        self.roll = other_orbit.roll
        self.quaternion_x = other_orbit.quaternion_x
        self.quaternion_y = other_orbit.quaternion_y
        self.quaternion_z = other_orbit.quaternion_z
        self.quaternion_q = other_orbit.quaternion_q
        self.crm = other_orbit.crm
        self.basename = other_orbit.basename
