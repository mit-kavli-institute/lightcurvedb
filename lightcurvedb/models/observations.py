import os

from sqlalchemy import Column, ForeignKey, SmallInteger, bindparam
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import relationship

from lightcurvedb.core.base_model import CreatedOnMixin, QLPModel
from lightcurvedb.core.constants import QLP_ORBITS
from lightcurvedb.core.datastructures.blob import Blobable


class Observation(QLPModel, CreatedOnMixin, Blobable):
    """
    This class allows easy queries between lightcurves and
    their observations per orbit.
    """

    __tablename__ = "observations"
    __abstract__ = False

    lightcurve_id = Column(
        ForeignKey("lightcurves.id", ondelete="CASCADE"),
        primary_key=True,
        nullable=False,
    )
    camera = Column(SmallInteger, index=True, nullable=False)
    ccd = Column(SmallInteger, index=True, nullable=False)
    orbit_id = Column(
        ForeignKey("orbits.id", ondelete="RESTRICT"),
        primary_key=True,
        nullable=False,
        index=True,
    )

    lightcurve = relationship("Lightcurve", back_populates="observations")
    orbit = relationship("Orbit")

    def __repr__(self):
        return "Observation Orbit-{0} Camera {1} CCD {2}, LC {3}".format(
            self.orbit.orbit_number, self.camera, self.ccd, self.lightcurve_id
        )

    @classmethod
    def upsert_q(cls):
        q = insert(cls).values(
            {
                cls.lightcurve_id: bindparam("lightcurve_id"),
                cls.camera: bindparam("camera"),
                cls.ccd: bindparam("ccd"),
                cls.orbit_id: bindparam("orbit_id"),
            }
        )
        q = q.on_conflict_do_nothing(
            constraint="observations_pkey",
        )

        return q

    def expected_orbit_h5_path(self, qlp_base_dir=QLP_ORBITS):
        """
        Determine the expected orbit h5 filepath for this observation.

        Parameters
        ----------
        qlp_base_dir : str or Path-like
            The filepath prefix to all QLP orbits.
        """
        h5_basename = "{0}.h5".format(self.tic_id)
        data_prefix = os.path.join(
            "orbit-{0}".format(self.orbit.orbit_number),
            "ffi",
            "cam{0}".format(self.camera),
            "ccd{0}".format(self.ccd),
            "LC",
        )
        return os.path.join(qlp_base_dir, data_prefix, h5_basename)

    def expected_sector_h5_path(self, qlp_base_dir=QLP_ORBITS):
        """
        Determine the expected sector h5 filepath for this observation.

        Parameters
        ----------
        qlp_base_dir : str or Path-like
            The filepath prefix to all QLP sectors.

        Notes
        -----
        This method assumes that all orbit data is aligned by sector. So long
        as the definition of a sector remains, this method will work.
        """
        h5_basename = "{0}.h5".format(self.tic_id)
        data_prefix = os.path.join(
            "sector-{0}".format(self.orbit.sector),
            "ffi",
            "cam{0}".format(self.camera),
            "ccd{0}".format(self.ccd),
            "LC",
        )
        return os.path.join(qlp_base_dir, data_prefix, h5_basename)
