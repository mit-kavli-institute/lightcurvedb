"""
lightcurve.py
=============
The lightcurve model module containing the Lightcurve model class
and directly related models
"""
from collections import OrderedDict

import numpy as np
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as psql
from sqlalchemy.orm import Mapped, mapped_column, relationship

from lightcurvedb.core.base_model import (
    CreatedOnMixin,
    NameAndDescriptionMixin,
    QLPModel,
)


class LightcurveType(QLPModel, CreatedOnMixin, NameAndDescriptionMixin):
    """Describes the numerous lightcurve types"""

    __tablename__ = "lightcurvetypes"

    id: Mapped[int] = mapped_column(sa.SmallInteger, primary_key=True)
    lightcurves: Mapped[list["ArrayOrbitLightcurve"]] = relationship(
        "ArrayOrbitLightcurve", back_populates="lightcurve_type"
    )

    def __str__(self):
        return self.name

    def __repr__(self):
        return f"<Lightcurve Type '{self.name}'>"


class ArrayOrbitLightcurve(QLPModel, CreatedOnMixin):
    """
    The latest model for representing Lightcurves. This model sacrifices
    lightpoint relational power for in-row arrays to optimize i/o operations.
    This also comes at the cost where developers must be sure to maintain
    cadence->value relations manually when editing lightcurves.

    Attributes
    ----------
    tic_id: int
        The TIC identifier of the parent star which was used to generate
        the Lightcurve's timeseries data.
    camera: int
        Which spacecraft camera this lightcurve was observed on.
    ccd: int
        Which spacecraft ccd this lightcurve was observed on.
    orbit_id: int
        The primary key of the orbit which this lightcurve was observed in.
    aperture_id: int
        The primary key of the aperture this lightcurve was generated with.
    lightcurve_type_id: int
        The primary key of the type of data this lightcurve represents.
        Usually this is raw lightcurves or from a variety of detrending
        methods.
    cadences: List[int]
        A list of observed cadences which have been recorded by the spacecraft.
    barycentric_julian_dates: List[float]
        A list of floats representing the time in days which the cadences were
        recorded on. This time is barycentric in reference.
    data: List[float]
        The time series value. The units of these values depend on the type
        and how they are interpreted is up to the developer.
    error: List[float]
        The error for each value. The values are left up to interpretation
        of the developer.
    x_centroids: List[float]
        Where, in terms of pixels on the ccd was the aperture centered on.
        With the exception of Background Lightcurves, this value is
        centered with a flux-weighted bias.
    y_centroids: List[float]
        Where, in terms of pixels on the ccd was the aperture centered on.
        With the exception of Background Lightcurves, this value is
        centered with a flux-weighted bias.
    quality_flags: List[int]
        The quality flags for the lightcurve.
    """

    __tablename__ = "array_orbit_lightcurves"

    DTYPE = OrderedDict(
        [
            ("cadences", "uint32"),
            ("barycentric_julian_dates", "float32"),
            ("data", "float64"),
            ("errors", "float64"),
            ("x_centroids", "float32"),
            ("y_centroids", "float32"),
            ("quality_flags", "uint16"),
        ]
    )
    tic_id: Mapped[int] = mapped_column(
        sa.BigInteger, primary_key=True, index=True
    )
    camera: Mapped[int] = mapped_column(
        sa.SmallInteger, primary_key=True, index=True
    )
    ccd: Mapped[int] = mapped_column(
        sa.SmallInteger, primary_key=True, index=True
    )
    orbit_id: Mapped[int] = mapped_column(
        sa.SmallInteger,
        sa.ForeignKey("orbits.id", onupdate="CASCADE", ondelete="CASCADE"),
        primary_key=True,
        index=True,
    )
    aperture_id: Mapped[int] = mapped_column(
        sa.SmallInteger,
        sa.ForeignKey("apertures.id", onupdate="CASCADE", ondelete="RESTRICT"),
        primary_key=True,
        index=True,
    )
    lightcurve_type_id: Mapped[int] = mapped_column(
        sa.SmallInteger,
        sa.ForeignKey("lightcurvetypes.id"),
        primary_key=True,
        index=True,
    )

    cadences = sa.Column(psql.ARRAY(sa.BigInteger, dimensions=1))
    barycentric_julian_dates = sa.Column(psql.ARRAY(sa.Float, dimensions=1))
    data = sa.Column(psql.ARRAY(psql.DOUBLE_PRECISION, dimensions=1))
    errors = sa.Column(psql.ARRAY(psql.DOUBLE_PRECISION, dimensions=1))
    x_centroids = sa.Column(psql.ARRAY(sa.Float, dimensions=1))
    y_centroids = sa.Column(psql.ARRAY(sa.Float, dimensions=1))
    quality_flags = sa.Column(psql.ARRAY(sa.Integer, dimensions=1))

    aperture = relationship("Aperture")
    lightcurve_type = relationship("LightcurveType")
    orbit = relationship("Orbit")

    @classmethod
    def create_structured_dtype(cls, *names):
        return list((name, cls.DTYPE[name]) for name in names)

    @classmethod
    def serialize_lightpoint_result(cls, db_result, *columns):
        dtype = cls.create_structured_dtype(*columns)
        return np.array(db_result, dtype=dtype)

    def get_tic_info(self, *fields):
        pass

    def to_numpy(self, normalize=False, offset=0.0):
        """
        Represent this lightcurve as a structured numpy array.

        Parameters
        ----------
        normalize: bool
            If true, normalize the lightcurve to the median of the
            ``data`` values.
        offset: float
            Offset the data values by adding this constant. By default this is
            0.0.
        """
        dtype = self.create_structured_dtype(*self.DTYPE.keys())

        fields = [getattr(self, col) for col in self.DTYPE.keys()]

        struct = np.array(list(zip(*fields)), dtype=dtype)

        if normalize:
            mask = struct["quality_flags"] == 0
            median = np.nanmedian(struct["data"][mask])
        else:
            median = 0.0

        struct["data"] = struct["data"] + (offset - median)
        return struct
