import os
from lightcurvedb.core.base_model import QLPModel

from sqlalchemy import (Column, BigInteger, Integer, SmallInteger, ForeignKey, bindparam)
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import insert


LEGACY_QLP_DIR = '/pdo/qlp-data'


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

    def expected_orbit_h5_path(self, qlp_base_dir=LEGACY_QLP_DIR):
        """
        Determine the expected orbit h5 filepath for this observation.

        Parameters
        ----------
        qlp_base_dir : str or Path-like
            The filepath prefix to all QLP orbits. Defaults to ``/pdo/qlp-data/``
        """
        h5_basename = '{}.h5'.format(self.tic_id)
        data_prefix = os.path.join(
            'orbit-{}'.format(self.orbit.orbit_number),
            'ffi',
            'cam{}'.format(self.camera),
            'ccd{}'.format(self.ccd),
            'LC'
        )
        return os.path.join(
            qlp_base_dir, data_prefix, h5_basename
        )

    def expected_sector_h5_path(self, qlp_base_dir=LEGACY_QLP_DIR):
        """
        Determine the expected sector h5 filepath for this observation.

        Parameters
        ----------
        qlp_base_dir : str or Path-like
            The filepath prefix to all QLP sectors. Defaults to ``/pdo/qlp-data/``

        Notes
        -----
        This method assumes that all orbit data is aligned by sector. So long
        as the definition of a sector remains, this method will work.
        """
        h5_basename = '{}.h5'.format(self.tic_id)
        data_prefix = os.path.join(
            'sector-{}'.format(self.orbit.sector),
            'ffi',
            'cam{}'.format(self.camera),
            'ccd{}'.format(self.ccd),
            'LC'
        )
        return os.path.join(
            qlp_base_dir, data_prefix, h5_basename
        )

