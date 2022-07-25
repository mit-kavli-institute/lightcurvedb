import os
from pathlib import Path

import numpy as np
import pandas as pd
from astropy.io import fits
from psycopg2 import extensions as ext
from sqlalchemy import (
    Boolean,
    Column,
    ForeignKey,
    Integer,
    Sequence,
    SmallInteger,
    String,
)
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.hybrid import hybrid_method, hybrid_property
from sqlalchemy.orm import relationship
from sqlalchemy.schema import CheckConstraint, UniqueConstraint
from sqlalchemy.sql.expression import cast

from lightcurvedb.core.base_model import QLPModel, CreatedOnMixin, NameAndDescriptionMixin
from lightcurvedb.core.fields import high_precision_column
from lightcurvedb.core.sql import psql_safe_str

FRAME_DTYPE = [
    ("cadence", np.int64),
    ("start_tjd", np.float64),
    ("mid_tjd", np.float64),
    ("end_tjd", np.float64),
    ("gps_time", np.float64),
    ("exp_time", np.float64),
    ("quality_bit", np.int32),
]


def adapt_pathlib(path):
    return ext.QuotedString(str(path))


ext.register_adapter(Path, adapt_pathlib)


class FrameType(QLPModel, CreatedOnMixin, NameAndDescriptionMixin):
    """Describes the numerous frame types"""

    __tablename__ = "frametypes"
    id = Column(SmallInteger, primary_key=True, unique=True)
    frames = relationship("Frame", back_populates="frame_type")

    def __repr__(self):
        return 'FrameType(name="{0}", description="{1}")'.format(
            self.name, self.description
        )


class Frame(QLPModel, CreatedOnMixin):
    """
    Provides ORM implementation of various Frame models
    """

    __tablename__ = "frames"

    # Constraints
    __table_args__ = (
        UniqueConstraint(
            "frame_type_id",
            "orbit_id",
            "cadence",
            "camera",
            "ccd",
            name="unique_frame",
        ),
        CheckConstraint(
            "camera BETWEEN 1 and 4", name="physical_camera_constraint"
        ),
        CheckConstraint(
            "(ccd IS NULL) OR (ccd BETWEEN 1 AND 4)",
            name="physical_ccd_constraint",
        ),
    )

    def __repr__(self):
        return (
            "<Frame {0} "
            "cam={1} "
            "ccd={2} "
            "cadence={3}>".format(
                self.frame_type_id, self.camera, self.ccd, self.cadence
            )
        )

    # Model attributes
    id = Column(
        Integer, Sequence("frames_id_seq", cache=2400), primary_key=True
    )
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

    _file_path = Column("file_path", String, nullable=False, unique=True)

    # Foreign Keys
    orbit_id = Column(
        Integer,
        ForeignKey("orbits.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    frame_type_id = Column(
        ForeignKey(FrameType.id, ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )

    # Relationships
    orbit = relationship("Orbit", back_populates="frames")
    frame_type = relationship("FrameType", back_populates="frames")
    lightcurves = association_proxy("lightcurveframemapping", "lightcurve")

    @classmethod
    def get_legacy_attrs(cls, dtype_override=None):
        if dtype_override:
            columns = dtype_override
        else:
            columns = FRAME_DTYPE

        return tuple(getattr(cls, column) for column, dtype in columns)

    @hybrid_method
    def cadence_type_in_minutes(self, clamp=False):
        """
        Return the cadence_type in minutes.

        Parameters
        ----------
        clamp: bool
            If true, clamp the value to an integer instead of returning as a
            float.

        Returns
        -------
        (float, int)
            The cadence_type in minutes. Return type is dependent on clamp. If
            clamp is specified return with an integer otherwise as a float.
        """
        return self.cadence_type // 60 if clamp else self.cadence_type / 60

    @cadence_type_in_minutes.expression
    def cadence_type_in_minutes(cls, clamp=False):
        """
        Evaluate an expression using cadence_type in minutes.

        Parameters
        ----------
        clamp: bool
            If true, clamp the value to an integer using an SQL type cast.

        Example
        -------
        >>> with db:
            # Get Frames with a 30 minute cadence type
            q = (
                db
                .query(Frame)
                .filter(Frame.cadence_type_in_minutes(clamp=True) == 30)
            )
            print(q.all())
        """
        param = cls.cadence_type / 60
        if clamp:
            return cast(param, Integer)
        return param

    def copy(self, other):
        self.cadence_type = other.cadence_type
        self.camera = other.camera
        self.ccd = other.ccd
        self.cadence = other.cadence
        self.gps_time = other.gps_time
        self.start_tjd = other.start_tjd
        self.mid_tjd = other.mid_tjd
        self.end_tjd = other.end_tjd
        self.exp_time = other.exp_time
        self.quality_bit = other.quality_bit
        self.file_path = other.file_path
        self.orbit = other.orbit
        self.frame_type = other.frame_type

    @classmethod
    def from_fits(cls, path, cadence_type=30, frame_type=None, orbit=None):
        abspath = os.path.abspath(path)
        header = fits.open(abspath)[0].header
        try:
            return cls(
                cadence_type=cadence_type,
                camera=header.get("CAM", header.get("CAMNUM", None)),
                ccd=header.get("CCD", header.get("CCDNUM", None)),
                cadence=header["CADENCE"],
                gps_time=header["TIME"],
                start_tjd=header["STARTTJD"],
                mid_tjd=header["MIDTJD"],
                end_tjd=header["ENDTJD"],
                exp_time=header["EXPTIME"],
                quality_bit=header["QUAL_BIT"],
                file_path=abspath,
                frame_type=frame_type,
                orbit=orbit,
            )
        except KeyError as e:
            print(e)
            print("===LOADED HEADER===")
            print(repr(header))
            raise

    @hybrid_property
    def file_path(self):
        return self._file_path

    @file_path.setter
    def file_path(self, value):
        self._file_path = psql_safe_str(value)

    @file_path.expression
    def file_path(cls):
        return cls._file_path

    @property
    def data(self):
        return fits.open(self.file_path)[0].data

    @hybrid_property
    def tjd(self):
        return self.mid_tjd

    @tjd.expression
    def tjd(cls):
        return cls.mid_tjd


class FrameAPIMixin(object):
    """
    Provide methods which iteract with the Frame table
    """

    def get_mid_tjd_mapping(self, frame_type="Raw FFI"):
        cameras = self.query(Frame.camera).distinct().all()
        mapping = {}
        for (camera,) in cameras:
            q = self.query(Frame.cadence, Frame.mid_tjd).filter(
                Frame.camera == camera, Frame.frame_type_id == frame_type
            )
            df = pd.read_sql(
                q.statement, self.bind, index_col=["cadence"]
            ).sort_index()
            mapping[camera] = df
        return mapping
