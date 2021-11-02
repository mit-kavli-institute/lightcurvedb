import os

import numpy as np
import pandas as pd

from astropy.io import fits
from lightcurvedb.core.base_model import QLPDataProduct, QLPDataSubType
from lightcurvedb.core.fields import high_precision_column
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
from sqlalchemy.orm import relationship
from sqlalchemy.schema import CheckConstraint, UniqueConstraint

FRAME_DTYPE = [
    ("cadence", np.int64),
    ("start_tjd", np.float64),
    ("mid_tjd", np.float64),
    ("end_tjd", np.float64),
    ("gps_time", np.float64),
    ("exp_time", np.float64),
    ("quality_bit", np.int32),
]


class FrameType(QLPDataSubType):
    """Describes the numerous frame types"""

    __tablename__ = "frametypes"

    frames = relationship("Frame", back_populates="frame_type")

    def __repr__(self):
        return 'FrameType(name="{0}", description="{1}")'.format(
            self.name, self.description
        )


class Frame(QLPDataProduct):
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

    file_path = Column(String, nullable=False, unique=True)

    # Foreign Keys
    orbit_id = Column(
        Integer,
        ForeignKey("orbits.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    frame_type_id = Column(
        ForeignKey("frametypes.name", ondelete="RESTRICT"),
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
