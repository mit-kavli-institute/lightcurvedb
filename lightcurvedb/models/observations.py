from lightcurvedb.core.base_model import QLPModel

from sqlalchemy import (Column, BigInteger, Integer, SmallInteger, ForeignKey, bindparam)
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import insert


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

    @classmethod
    def upsert_dicts(cls):
        q = insert(cls).values({
            cls.tic_id: bindparam('tic_id'),
            cls.camera: bindparam('camera'),
            cls.ccd: bindparam('ccd'),
            cls.orbit_id: bindparam('orbit_id')
        })
        q = q.on_conflict_do_update(
            constraint='observations_pkey',
            set_=dict(
                camera=q.excluded.camera,
                ccd=q.excluded.ccd
            )
        )

        return q
