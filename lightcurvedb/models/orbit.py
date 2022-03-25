import os
import re
from multiprocessing import Pool

import click
import numpy as np
from astropy.io import fits
from sqlalchemy import (
    Boolean,
    Column,
    Integer,
    Sequence,
    String,
    func,
    inspect,
    select,
)
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship

from lightcurvedb.core.base_model import QLPReference
from lightcurvedb.core.constants import POC_ORBITS, QLP_ORBITS, QLP_SECTORS
from lightcurvedb.core.fields import high_precision_column
from lightcurvedb.core.sql import psql_safe_str
from lightcurvedb.models import CameraQuaternion, Frame, Observation


def _extr_fits_header(f):
    return fits.open(f)[0].header


ORBIT_DTYPE = [
    ("orbit_number", np.int32),
    ("crm_n", np.int16),
    ("right_ascension", np.float64),
    ("declination", np.float64),
    ("roll", np.float64),
    ("quaternion_x", np.float64),
    ("quaternion_y", np.float64),
    ("quaternion_z", np.float64),
    ("quaternion_q", np.float64),
]


class Orbit(QLPReference):
    """
    Provides ORM implementation of an orbit completed by TESS
    """

    __tablename__ = "orbits"

    # Model Attributes
    id = Column(Integer, Sequence("orbit_id_seq"), primary_key=True)
    orbit_number = Column(Integer, unique=True, nullable=False)
    sector = Column(Integer, nullable=False)

    right_ascension = high_precision_column(nullable=False)
    declination = high_precision_column(nullable=False)
    roll = high_precision_column(nullable=False)

    quaternion_x = high_precision_column(nullable=False)
    quaternion_y = high_precision_column(nullable=False)
    quaternion_z = high_precision_column(nullable=False)
    quaternion_q = high_precision_column(nullable=False)

    crm = Column(Boolean, nullable=False)  # Has been correct for CRM
    crm_n = Column(Integer, nullable=False)  # Cosmic Ray Mitigation Number
    _basename = Column("basename", String(256), nullable=False)

    # Relationships
    frames = relationship("Frame", back_populates="orbit")
    observations = relationship(Observation, back_populates="orbit")
    # Click Parameters
    click_parameters = click.Choice(
        ["orbit_number", "sector", "ra", "dec", "roll", "basename"],
        case_sensitive=False,
    )

    def __repr__(self):
        return "Orbit-{0} Sector-{1} ({2:.3f}, {3:.3f}, {4:.3f}) {5}".format(
            self.orbit_number,
            self.sector,
            self.right_ascension,
            self.declination,
            self.roll,
            self.basename,
        )

    @classmethod
    def get_legacy_attrs(cls, dtype_override=None):
        if dtype_override:
            columns = dtype_override
        else:
            columns = ORBIT_DTYPE
        return tuple(getattr(cls, column) for column, dtype in columns)

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

    @classmethod
    def generate_from_fits(cls, files, parallel=True):
        if parallel:
            with Pool() as p:
                headers = p.map(_extr_fits_header, files)
        else:
            headers = [fit[0].header for fit in files]

        # Check that all headers are congruent for the orbit
        require_congruency_map = {
            "ORBIT_ID": "orbit_number",
            "SC_RA": "right_ascension",
            "SC_DEC": "declination",
            "SC_ROLL": "roll",
            "SC_QUATX": "quaternion_x",
            "SC_QUATY": "quaternion_y",
            "SC_QUATZ": "quaternion_z",
            "SC_QUATQ": "quaternion_q",
            "CRM": "crm",
            "CRM_N": "crm_n",
        }

        for column in require_congruency_map.keys():
            assert all(
                headers[0].get(column) == cmpr.get(column)
                for cmpr in headers[1:]
            )

        basename = re.search(
            r"(?P<basename>tess[0-9]+)", files[0]
        ).groupdict()["basename"]

        attrs = {v: headers[0][k] for k, v in require_congruency_map.items()}
        attrs["basename"] = basename

        return cls(**attrs)

    @hybrid_property
    def max_cadence(self):
        cadences = {f.cadence for f in self.frames}
        return max(cadences)

    @hybrid_property
    def min_cadence(self):
        cadences = {f.cadence for f in self.frames}
        return min(cadences)

    @hybrid_property
    def min_gps_time(self):
        return min(f.gps_time for f in self.frames)

    @hybrid_property
    def max_gps_time(self):
        return max(f.gps_time for f in self.frames)

    @hybrid_property
    def ra(self):
        return self.right_ascension

    @hybrid_property
    def dec(self):
        return self.declination

    @hybrid_property
    def cadences(self):
        return [f.cadence for f in self.frames]

    @cadences.expression
    def cadences(cls):
        return Frame.cadence

    @max_cadence.expression
    def max_cadence(cls):
        return func.max(Frame.cadence).label("max_cadence")

    @min_cadence.expression
    def min_cadence(cls):
        return func.min(Frame.cadence).label("min_cadence")

    @min_gps_time.expression
    def min_gps_time(cls):
        q = (
            select([Frame.gps_time])
            .where(Frame.orbit_id == cls.id)
            .order_by(Frame.cadence.asc())
            .limit(1)
            .label("min_gps_time")
        )

        return q

    @max_gps_time.expression
    def max_gps_time(cls):
        q = (
            select([Frame.gps_time])
            .where(Frame.orbit_id == cls.id)
            .order_by(Frame.cadence.desc())
            .limit(1)
            .label("max_gps_time")
        )

        return q

    @ra.expression
    def ra(cls):
        return cls.right_ascension

    @dec.expression
    def dec(cls):
        return cls.declination

    @hybrid_property
    def basename(self):
        return self._basename

    @basename.setter
    def basename(self, value):
        """Sanitize"""
        self._basename = psql_safe_str(value)

    def get_qlp_directory(self, base_path=QLP_ORBITS, suffixes=None):
        """
        Return the base QLP orbit directory for the orbit
        """
        return os.path.join(
            base_path,
            "orbit-{0}".format(self.orbit_number),
            *suffixes if suffixes else []
        )

    def get_sector_directory(self, *suffixes):
        base_sector_dir = QLP_SECTORS.format(sector=self.sector)
        return os.path.join(base_sector_dir, *suffixes)

    def get_poc_directory(self, base_path=POC_ORBITS, suffixes=None):
        """
        Return the base POC orbit directory for the orbit.
        """

        return os.path.join(
            base_path,
            "orbit-{0}".format(self.orbit_number),
            *suffixes if suffixes else []
        )

    def get_qlp_run_directory(self, base_path="/pdo/qlp-data"):
        base_dir = self.get_qlp_directory(base_path)
        run_dir = os.path.join(base_dir, "ffi", "run")
        return run_dir

    def get_camera_quaternions(self, *cameras):
        """
        Build a query that returns the camera quaternions that were recorded
        for this orbit's gps time limits.

        Parameters
        ----------
        *cameras : int, variable
            Camera discriminators to pass. If left empty (default) then no
            cameras will be filtered for and all quaternions in this orbit
            will be returned.
        """
        session = inspect(self).session
        max_gps_time = self.max_gps_time
        min_gps_time = self.min_gps_time

        q = session.query(CameraQuaternion).filter(
            CameraQuaternion.gps_time.between(min_gps_time, max_gps_time)
        )

        if len(cameras) == 1:
            q = q.filter(CameraQuaternion.camera == cameras[0])
        elif len(cameras) > 1:
            q = q.filter(CameraQuaternion.camera.in_(cameras))
        return q
