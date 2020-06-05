from lightcurvedb.core.base_model import QLPModel

from sqlalchemy import (Column, BigInteger, Integer, SmallInteger, ForeignKey)
from sqlalchemy.orm import relationship


class Observation(QLPModel):
    """
        This class allows easy queries between lightcurves and
        their observations per orbit.
    """
    __tablename__ = 'observations'

    tic_id = Column(BigInteger, primary_key=True, nullable=False)
    camera = Column(SmallInteger, index=True, nullable=False)
    ccd = Column(SmallInteger, index=True, nullable=False)
    orbit_id = Column(
        ForeignKey('orbits.id', ondelete='RESTRICT'), primary_key=True, nullable=False
    )

    orbit = relationship('Orbit', back_populates='observations')
