import os
from pathlib import Path

import numpy as np
from astropy.io import fits
from psycopg2 import extensions as ext
from sqlalchemy import (
    Boolean,
    Column,
    Float,
    ForeignKey,
    Integer,
    Sequence,
    SmallInteger,
    String,
)
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.hybrid import hybrid_method, hybrid_property
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.schema import CheckConstraint, UniqueConstraint
from sqlalchemy.sql.expression import cast

from lightcurvedb.core.base_model import (
    CreatedOnMixin,
    NameAndDescriptionMixin,
    QLPModel,
)
from lightcurvedb.core.sql import psql_safe_str


def adapt_pathlib(path):
    return ext.QuotedString(str(path))


ext.register_adapter(Path, adapt_pathlib)


_FRAME_MAPPER_LOOKUP = {
    "INT_TIME": ("cadence_type", Column(SmallInteger, index=True)),
    "CAM": ("camera", Column(SmallInteger, index=True)),
    "CAMNUM": ("camera", Column(SmallInteger, index=True, nullable=True)),
    "CCD": ("ccd", Column(SmallInteger, index=True, nullable=True)),
    "CADENCE": ("cadence", Column(Integer, index=True)),
    "TIME": ("gps_time", Column(DOUBLE_PRECISION)),
    "STARTTJD": ("start_tjd", Column(DOUBLE_PRECISION)),
    "MIDTJD": ("mid_tjd", Column(DOUBLE_PRECISION)),
    "ENDTJD": ("end_tjd", Column(DOUBLE_PRECISION)),
    "EXPTIME": ("exp_time", Column(DOUBLE_PRECISION)),
    "QUAL_BIT": ("quality_bit", Column(Boolean)),
    "FINE": ("fine_pointing", Column(Boolean)),
    "COARSE": ("coarse_pointing", Column(Boolean)),
    "RW_DESAT": ("reaction_wheel_desaturation", Column(Boolean)),
    "SIMPLE": ("simple", Column(Boolean, nullable=True)),
    "BITPIX": ("bit_pix", Column(SmallInteger, nullable=True)),
    "NAXIS": ("n_axis", Column(SmallInteger, nullable=True)),
    "EXTENDED": ("extended", Column(Boolean, nullable=True)),
    "ACS_MODE": ("acs_mode", Column(String, nullable=True)),
    "PIX_CAT": ("pix_cat", Column(Integer, nullable=True)),
    "REQUANT": ("requant", Column(Integer, nullable=True)),
    "DIFF_HUF": ("huffman_difference", Column(Integer, nullable=True)),
    "PRIM_HUF": ("huffman_prime", Column(Integer, nullable=True)),
    "SPM": ("spm", Column(Integer, nullable=True)),
    "CRM": ("cosmic_ray_mitigation", Column(Boolean, nullable=True)),
    "ORB_SEG": ("orbital_segment", Column(String, nullable=True)),
    "SCIPIXS": ("science_pixels", Column(String, nullable=True)),
    "GAIN_A": ("gain_a", Column(Float, nullable=True)),
    "GAIN_B": ("gain_b", Column(Float, nullable=True)),
    "GAIN_C": ("gain_c", Column(Float, nullable=True)),
    "GAIN_D": ("gain_d", Column(Float, nullable=True)),
    "UNITS": ("units", Column(String, nullable=True)),
    "EQUINOX": ("equinox", Column(Float, nullable=True)),
    "INSTRUME": ("instrument", Column(String, nullable=True)),
    "TELESCOP": ("telescope", Column(String, nullable=True)),
    "MJD-BEG": ("mjd-beg", Column(Float, nullable=True)),
    "MJD-END": ("mjd-end", Column(Float, nullable=True)),
    "TESS_X": ("tess_x_position", Column(Float, nullable=True)),
    "TESS_Y": ("tess_y_position", Column(Float, nullable=True)),
    "TESS_Z": ("tess_z_position", Column(Float, nullable=True)),
    "TESS_VX": ("tess_x_velocity", Column(Float, nullable=True)),
    "TESS_VY": ("tess_y_velocity", Column(Float, nullable=True)),
    "TESS_VZ": ("tess_z_velocity", Column(Float, nullable=True)),
    "RA_TARG": ("target_ra", Column(Float, nullable=True)),
    "DEC_TARG": ("target_dec", Column(Float, nullable=True)),
    "WCSGDF": ("wcsgdf", Column(Float, nullable=True)),
    "CHECKSUM": ("checksum", Column(String, nullable=True)),
    "DATASUM": ("datasum", Column(Integer, nullable=True)),
    "COMMENT": ("comment", Column(String, nullable=True)),
}


class FrameType(QLPModel, CreatedOnMixin, NameAndDescriptionMixin):
    """Describes the numerous frame types"""

    __tablename__ = "frametypes"
    id: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    frames = relationship("Frame", back_populates="frame_type")

    def __repr__(self):
        return 'FrameType(name="{0}", description="{1}")'.format(
            self.name, self.description
        )


class FrameFFIMapper(QLPModel.__class__):
    def __new__(cls, name, bases, attrs):
        # Dynamically assign FFI fields, their translations, and
        # fallback FFI Keyword
        for ffi_name, (model_name, col) in _FRAME_MAPPER_LOOKUP.items():
            if model_name not in attrs:
                attrs[model_name] = col  # Avoid redefinitions
            # Define hybrid properties
            fallback = ffi_name.replace("-", "_")
            setter_name = f"_{model_name}_setter"
            expr_name = f"_{model_name}_expression"

            attrs[fallback] = hybrid_property(
                lambda self: getattr(self, model_name)
            )
            attrs[setter_name] = attrs[fallback].inplace.setter(
                lambda self, value: setattr(self, model_name, value)
            )
            attrs[expr_name] = attrs[fallback].inplace.expression(
                classmethod(lambda cls: getattr(cls, model_name))
            )
        return super().__new__(cls, name, bases, attrs)


class Frame(QLPModel, CreatedOnMixin, metaclass=FrameFFIMapper):
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

    FRAME_DTYPE = [
        ("cadence", np.int64),
        ("start_tjd", np.float64),
        ("mid_tjd", np.float64),
        ("end_tjd", np.float64),
        ("gps_time", np.float64),
        ("exp_time", np.float64),
        ("quality_bit", np.int32),
    ]
    FRAME_COMP_DTYPE = [("orbit_id", np.int32)] + FRAME_DTYPE

    # Model attributes
    id: Mapped[int] = mapped_column(
        Sequence("frames_id_seq", cache=2400), primary_key=True
    )
    _file_path = Column("file_path", String, nullable=False, unique=True)

    # Foreign Keys
    orbit_id: Mapped[int] = mapped_column(
        ForeignKey("orbits.id", ondelete="RESTRICT"),
        index=True,
    )
    frame_type_id: Mapped[int] = mapped_column(
        ForeignKey(FrameType.id, ondelete="RESTRICT"),
        index=True,
    )

    # Relationships
    orbit = relationship("Orbit", back_populates="frames")
    frame_type = relationship("FrameType", back_populates="frames")
    lightcurves = association_proxy("lightcurveframemapping", "lightcurve")

    @hybrid_property
    def file_path(self):
        return self._file_path

    @file_path.inplace.setter
    def _file_path_setter(self, value):
        self._file_path = psql_safe_str(value)

    @file_path.inplace.expression
    @classmethod
    def _file_path_expression(cls):
        return cls._file_path

    @classmethod
    def get_legacy_attrs(cls, dtype_override=None):
        if dtype_override:
            columns = dtype_override
        else:
            columns = cls.FRAME_DTYPE

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

    @cadence_type_in_minutes.inplace.expression
    @classmethod
    def _cadence_type_in_minutes(cls, clamp=False):
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

    @hybrid_property
    def tjd(self):
        return self.mid_tjd

    @tjd.inplace.expression
    @classmethod
    def _tjd(cls):
        return cls.mid_tjd

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
