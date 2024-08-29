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

_FRAME_MAPPER_LOOKUP = {
    "INT_TIME": ("cadence_type", sa.Column(sa.SmallInteger, index=True)),
    "CAM": ("camera", sa.Column(sa.SmallInteger, index=True)),
    "CAMNUM": (
        "camera",
        sa.Column(sa.SmallInteger, index=True, nullable=True),
    ),
    "CCD": ("ccd", sa.Column(sa.SmallInteger, index=True, nullable=True)),
    "CADENCE": ("cadence", sa.Column(sa.Integer, index=True)),
    "TIME": ("gps_time", sa.Column(DOUBLE_PRECISION)),
    "STARTTJD": ("start_tjd", sa.Column(DOUBLE_PRECISION)),
    "MIDTJD": ("mid_tjd", sa.Column(DOUBLE_PRECISION)),
    "ENDTJD": ("end_tjd", sa.Column(DOUBLE_PRECISION)),
    "EXPTIME": ("exp_time", sa.Column(DOUBLE_PRECISION)),
    "QUAL_BIT": ("quality_bit", sa.Column(sa.Boolean)),
    "FINE": ("fine_pointing", sa.Column(sa.Boolean)),
    "COARSE": ("coarse_pointing", sa.Column(sa.Boolean)),
    "RW_DESAT": ("reaction_wheel_desaturation", sa.Column(sa.Boolean)),
    "SIMPLE": ("simple", sa.Column(sa.Boolean, nullable=True)),
    "BITPIX": ("bit_pix", sa.Column(sa.SmallInteger, nullable=True)),
    "NAXIS": ("n_axis", sa.Column(sa.SmallInteger, nullable=True)),
    "EXTENDED": ("extended", sa.Column(sa.Boolean, nullable=True)),
    "ACS_MODE": ("acs_mode", sa.Column(sa.String, nullable=True)),
    "PIX_CAT": ("pix_cat", sa.Column(sa.Integer, nullable=True)),
    "REQUANT": ("requant", sa.Column(sa.Integer, nullable=True)),
    "DIFF_HUF": ("huffman_difference", sa.Column(sa.Integer, nullable=True)),
    "PRIM_HUF": ("huffman_prime", sa.Column(sa.Integer, nullable=True)),
    "SPM": ("spm", sa.Column(sa.Integer, nullable=True)),
    "CRM": ("cosmic_ray_mitigation", sa.Column(sa.Boolean, nullable=True)),
    "ORB_SEG": ("orbital_segment", sa.Column(sa.String, nullable=True)),
    "SCIPIXS": ("science_pixels", sa.Column(sa.String, nullable=True)),
    "GAIN_A": ("gain_a", sa.Column(sa.Float, nullable=True)),
    "GAIN_B": ("gain_b", sa.Column(sa.Float, nullable=True)),
    "GAIN_C": ("gain_c", sa.Column(sa.Float, nullable=True)),
    "GAIN_D": ("gain_d", sa.Column(sa.Float, nullable=True)),
    "UNITS": ("units", sa.Column(sa.String, nullable=True)),
    "EQUINOX": ("equinox", sa.Column(sa.Float, nullable=True)),
    "INSTRUME": ("instrument", sa.Column(sa.String, nullable=True)),
    "TELESCOP": ("telescope", sa.Column(sa.String, nullable=True)),
    "MJD-BEG": ("mjd-beg", sa.Column(sa.Float, nullable=True)),
    "MJD-END": ("mjd-end", sa.Column(sa.Float, nullable=True)),
    "TESS_X": ("tess_x_position", sa.Column(sa.Float, nullable=True)),
    "TESS_Y": ("tess_y_position", sa.Column(sa.Float, nullable=True)),
    "TESS_Z": ("tess_z_position", sa.Column(sa.Float, nullable=True)),
    "TESS_VX": ("tess_x_velocity", sa.Column(sa.Float, nullable=True)),
    "TESS_VY": ("tess_y_velocity", sa.Column(sa.Float, nullable=True)),
    "TESS_VZ": ("tess_z_velocity", sa.Column(sa.Float, nullable=True)),
    "RA_TARG": ("target_ra", sa.Column(sa.Float, nullable=True)),
    "DEC_TARG": ("target_dec", sa.Column(sa.Float, nullable=True)),
    "WCSGDF": ("wcsgdf", sa.Column(sa.Float, nullable=True)),
    "CHECKSUM": ("checksum", sa.Column(sa.String, nullable=True)),
    "DATASUM": ("datasum", sa.Column(sa.Integer, nullable=True)),
    "COMMENT": ("comment", sa.Column(sa.String, nullable=True)),
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


class FrameFFIMapper(QLPModel.__class__):
    """
    It's really hard mapping relevant FFI headers to SQL Models as they
    are numerous. So instead, reference the schema defined in
    ``_FRAME_MAPPER_LOOKUP`` to build the fallback functions.

    The keys within the schema are the expected names within the FFI
    headers. The values are a 2 element tuple with the first element
    being the human-readable column name. This name must be a valid
    python class attribute name. The second element is the compatible
    SQL datatype that can represent the corresponding values within the
    FFI header.

    The mapping is performed via a metaclass in order to dynamically
    assign methods to the decorated class.
    """

    def __new__(cls, name, bases, attrs):
        def fallback_func(model_name: str):
            @hybrid_property
            def method(self):
                return getattr(self, model_name)

            return method

        def setter_func(method, model_name):
            @method.inplace.setter
            def setter(self, value):
                setattr(self, model_name, value)

            return setter

        def expression_func(method, model_name):
            @method.inplace.expression
            @classmethod
            def expression(cls):
                return getattr(cls, model_name)

            return expression

        # Dynamically assign FFI fields, their translations, and
        # fallback FFI Keyword
        for ffi_name, (model_name, col) in _FRAME_MAPPER_LOOKUP.items():
            if model_name not in attrs:
                attrs[model_name] = col  # Avoid redefinitions

            # Define hybrid properties
            fallback = ffi_name.replace("-", "_")
            setter_name = f"_{model_name}_setter"
            expr_name = f"_{model_name}_expression"

            fallback_method = fallback_func(model_name)
            setter_method = setter_func(fallback_method, model_name)
            expr_method = expression_func(fallback_method, model_name)

            attrs[fallback] = fallback_method
            attrs[setter_name] = setter_method
            attrs[expr_name] = expr_method
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
        sa.Sequence("frames_id_seq", cache=2400), primary_key=True
    )
    stray_light: Mapped[Optional[bool]]
    _file_path = sa.Column("file_path", sa.String, nullable=False, unique=True)

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
