"""
lightcurve.py
=============
The lightcurve model module containing the Lightcurve model class
and directly related models
"""
from collections import OrderedDict

import numpy as np
import sqlalchemy as sa
from psycopg2.extensions import AsIs, register_adapter
from sqlalchemy.dialects import postgresql as psql
from sqlalchemy.orm import relationship

from lightcurvedb.core.base_model import (
    CreatedOnMixin,
    NameAndDescriptionMixin,
    QLPModel,
)
from lightcurvedb.models.aperture import Aperture
from lightcurvedb.models.orbit import Orbit


def adapt_as_is_type(type_class):
    def adaptor(type_instance):
        return AsIs(type_instance)

    register_adapter(type_class, adaptor)


adapt_as_is_type(np.int64)
adapt_as_is_type(np.int32)
adapt_as_is_type(np.float32)
adapt_as_is_type(np.float64)


class LightcurveType(QLPModel, CreatedOnMixin, NameAndDescriptionMixin):
    """Describes the numerous lightcurve types"""

    __tablename__ = "lightcurvetypes"

    id = sa.Column(sa.SmallInteger, primary_key=True, unique=True)
    lightcurves = relationship(
        "ArrayLightcurve", back_populates="lightcurve_type"
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
    tic_id = sa.Column(sa.BigInteger, primary_key=True, index=True)
    camera = sa.Column(sa.SmallInteger, primary_key=True, index=True)
    ccd = sa.Column(sa.SmallInteger, primary_key=True, index=True)
    orbit_id = sa.Column(
        sa.SmallInteger,
        sa.ForeignKey("orbits.id", onupdate="CASCADE", ondelete="CASCADE"),
        primary_key=True,
        index=True,
    )
    aperture_id = sa.Column(
        sa.SmallInteger,
        sa.ForeignKey("apertures.id", onupdate="CASCADE", ondelete="RESTRICT"),
        primary_key=True,
        index=True,
    )
    lightcurve_type_id = sa.Column(
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


class ArrayOrbitLightcurveAPIMixin:
    def _process_lc_selection(self, select_q):
        from lightcurvedb.core.tic8 import one_off

        structs = []
        stellar_param_info = {}
        for lc in self.execute(select_q).scalars():
            try:
                tmag = stellar_param_info[lc.tic_id]
            except KeyError:
                tmag = one_off(lc.tic_id, "tmag")[0]
                stellar_param_info[lc.tic_id] = tmag
            structs.append(lc.to_numpy(normalize=True, offset=tmag))
        return np.concatenate(structs)

    def get_missing_id_ranges(self):
        """
        Parse through all orbit lightcurves to find gaps in the primary key
        sequence.

        Returns
        -------
        List[(int, int)]
        Returns a list of tuples indicating lack of assigned ids in the range
        of [start, end].

        Note
        ----
        This query must parse all ids and hence can take a few minutes to
        complete. As such, if pipeline operations are occuring, this list
        may not include the newest gaps.
        """
        subq = sa.select(
            ArrayOrbitLightcurve.id,
            (
                sa.func.lead(ArrayOrbitLightcurve.id)
                .over(order_by=ArrayOrbitLightcurve.id)
                .label("next_id")
            ),
        ).subquery()
        q = sa.select(
            (subq.c.id + 1).label("gap_start"),
            (subq.c.next_id - 1).label("gap_end"),
        ).where(subq.c.id + 1 != subq.c.next_id)
        return self.execute(q).fetchall()

    def get_lightcurve(
        self, tic_id, lightcurve_type, aperture, orbit, resolve=True
    ):
        """
        Retrieves a single lightcurve row.

        Arguments
        ---------
        tic_id: int
            The TIC id of the desired lightcurve.
        aperture: str or integer
            The aperture name or aperture id of the desired lightcurve.
        lightcurve_type: str or integer
            The type name or type id of the desired lightcurve.
        orbit: integer
            The physical orbit number of the desired lightcurve.
        resolve: bool, optional
            If True, return the resolved lightcurve query, otherwise return
            the query object itself.

        Returns
        -------
        OrbitLightcurve, sqlalchemy.Query

        Raises
        ------
        sqlalchemy.orm.exc.NoResultFound
            No lightcurve matched the desired parameters.
        """

        q = (
            sa.select(ArrayOrbitLightcurve)
            .join(ArrayOrbitLightcurve.orbit)
            .filter(Orbit.orbit_number == orbit)
        )

        if isinstance(aperture, str):
            q = q.join(ArrayOrbitLightcurve.aperture)
            q = q.filter(ArrayOrbitLightcurve.aperture_name == aperture)
        else:
            q = q.filter(ArrayOrbitLightcurve.aperture_id == aperture)

        if isinstance(lightcurve_type, str):
            q = q.join(ArrayOrbitLightcurve.lightcurve_type)
            q = q.filter(
                ArrayOrbitLightcurve.lightcurve_type_name == lightcurve_type
            )
        else:
            q = q.filter(
                ArrayOrbitLightcurve.lightcurve_type_id == lightcurve_type
            )

        if resolve:
            return q.one()

        return q

    def get_lightcurve_baseline(self, tic_id, lightcurve_type, aperture):
        q = (
            sa.select(ArrayOrbitLightcurve)
            .join(ArrayOrbitLightcurve.aperture)
            .join(ArrayOrbitLightcurve.lightcurve_type)
            .join(ArrayOrbitLightcurve.orbit)
            .where(
                ArrayOrbitLightcurve.tic_id == tic_id,
                Aperture.name == aperture,
                LightcurveType.name == lightcurve_type,
            )
            .order_by(Orbit.orbit_number)
        )
        return self._process_lc_selection(q)
