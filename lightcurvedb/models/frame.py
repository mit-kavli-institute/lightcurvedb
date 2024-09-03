from typing import Optional

import numpy as np
import sqlalchemy as sa
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

FRAME_MAPPER_LOOKUP = {
    "INT_TIME": "cadence_type",
    "CAM": "camera",
    "CAMNUM": "camera",
    "CCD": "ccd",
    "CADENCE": "cadence",
    "TIME": "gps_time",
    "STARTTJD": "start_tjd",
    "MIDTJD": "mid_tjd",
    "ENDTJD": "end_tjd",
    "EXPTIME": "exposure_time",
    "QUAL_BIT": "quality_bit",
    "FINE": "fine_pointing",
    "COARSE": "coarse_pointing",
    "RW_DESAT": "reaction_wheel_desaturation",
    "SIMPLE": "simple",
    "BITPIX": "bit_pix",
    "NAXIS": "n_axis",
    "EXTENDED": "extended",
    "ACS_MODE": "acs_mode",
    "PIX_CAT": "pix_cat",
    "REQUANT": "requant",
    "DIFF_HUF": "huffman_difference",
    "PRIM_HUF": "huffman_prime",
    "SPM": "spm",
    "CRM": "cosmic_ray_mitigation",
    "ORB_SEG": "orbital_segment",
    "SCIPIXS": "science_pixels",
    "GAIN_A": "gain_a",
    "GAIN_B": "gain_b",
    "GAIN_C": "gain_c",
    "GAIN_D": "gain_d",
    "UNITS": "units",
    "EQUINOX": "equinox",
    "INSTRUME": "instrument",
    "TELESCOP": "telescope",
    "MJD-BEG": "mjd-beg",
    "MJD-END": "mjd-end",
    "TESS_X": "tess_x_position",
    "TESS_Y": "tess_y_position",
    "TESS_Z": "tess_z_position",
    "TESS_VX": "tess_x_velocity",
    "TESS_VY": "tess_y_velocity",
    "TESS_VZ": "tess_z_velocity",
    "RA_TARG": "target_ra",
    "DEC_TARG": "target_dec",
    "WCSGDF": "wcsgdf",
    "CHECKSUM": "checksum",
    "DATASUM": "datasum",
    "COMMENT": "comment",
}


class FrameType(QLPModel, CreatedOnMixin, NameAndDescriptionMixin):
    """Describes the numerous frame types"""

    __tablename__ = "frametypes"
    id: Mapped[int] = mapped_column(sa.SmallInteger, primary_key=True)
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
        sa.Sequence("frames_id_seq", cache=2400), primary_key=True
    )
    stray_light: Mapped[Optional[bool]]
    _file_path = sa.Column("file_path", sa.String, nullable=False, unique=True)

    cadence: Mapped[int] = mapped_column(index=True)
    cadence_type: Mapped[int] = mapped_column(sa.SmallInteger)
    camera: Mapped[int] = mapped_column(sa.SmallInteger, index=True)
    ccd: Mapped[Optional[int]] = mapped_column(sa.SmallInteger, index=True)
    gps_time: Mapped[float] = mapped_column(DOUBLE_PRECISION)
    start_tjd: Mapped[float] = mapped_column(DOUBLE_PRECISION)
    mid_tjd: Mapped[float] = mapped_column(DOUBLE_PRECISION)
    end_tjd: Mapped[float] = mapped_column(DOUBLE_PRECISION)
    exposure_time: Mapped[float] = mapped_column(
        DOUBLE_PRECISION, name="exp_time"
    )
    quality_bit: Mapped[Optional[bool]]
    fine_pointing: Mapped[Optional[bool]]
    coarse_pointing: Mapped[Optional[bool]]
    reaction_wheel_desaturation: Mapped[Optional[bool]]
    simple: Mapped[Optional[bool]]
    bit_pix: Mapped[Optional[int]] = mapped_column(sa.SmallInteger)
    n_axis: Mapped[Optional[int]] = mapped_column(sa.SmallInteger)
    extended: Mapped[Optional[bool]]
    acs_mode: Mapped[Optional[str]]
    pix_cat: Mapped[Optional[int]]
    requant: Mapped[Optional[int]]
    huffman_difference: Mapped[Optional[int]]
    huffman_prime: Mapped[Optional[int]]
    spm: Mapped[Optional[int]]
    cosmic_ray_mitigation: Mapped[Optional[bool]]
    orbital_segment: Mapped[Optional[str]]
    science_pixels: Mapped[Optional[str]]
    gain_a: Mapped[Optional[float]]
    gain_b: Mapped[Optional[float]]
    gain_c: Mapped[Optional[float]]
    gain_d: Mapped[Optional[float]]
    units: Mapped[Optional[str]]
    equinox: Mapped[Optional[float]]
    instrument: Mapped[Optional[str]]
    telescope: Mapped[Optional[str]]
    mjd_beg: Mapped[Optional[float]]
    mjd_end: Mapped[Optional[float]]
    tess_x_position: Mapped[Optional[float]]
    tess_y_position: Mapped[Optional[float]]
    tess_z_position: Mapped[Optional[float]]
    tess_x_velocity: Mapped[Optional[float]]
    tess_y_velocity: Mapped[Optional[float]]
    tess_z_velocity: Mapped[Optional[float]]
    target_ra: Mapped[Optional[float]]
    target_dec: Mapped[Optional[float]]
    wcsgdf: Mapped[Optional[float]]
    checksum: Mapped[Optional[str]]
    datasum: Mapped[Optional[int]]
    comment: Mapped[Optional[str]]

    # Foreign Keys
    orbit_id: Mapped[int] = mapped_column(
        sa.ForeignKey("orbits.id", ondelete="RESTRICT"),
        index=True,
    )
    frame_type_id: Mapped[int] = mapped_column(
        sa.ForeignKey(FrameType.id, ondelete="RESTRICT"),
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
            return cast(param, sa.Integer)
        return param

    @hybrid_property
    def tjd(self):
        return self.mid_tjd

    @tjd.inplace.expression
    @classmethod
    def _tjd(cls):
        return cls.mid_tjd

    @hybrid_property
    def cam(self):
        return self.camera
