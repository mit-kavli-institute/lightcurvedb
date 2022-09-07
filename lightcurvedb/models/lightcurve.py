"""
lightcurve.py
=============
The lightcurve model module containing the Lightcurve model class
and directly related models
"""

import numpy as np
from psycopg2.extensions import AsIs, register_adapter
from sqlalchemy import (
    BigInteger,
    Column,
    ForeignKey,
    Integer,
    Sequence,
    SmallInteger,
    func,
    inspect,
    select,
)
from sqlalchemy.dialects.postgresql import aggregate_order_by
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship
from sqlalchemy.schema import UniqueConstraint

from lightcurvedb.core.base_model import (
    CreatedOnMixin,
    NameAndDescriptionMixin,
    QLPModel,
)
from lightcurvedb.models.lightpoint import LIGHTPOINT_NP_DTYPES, Lightpoint


def adapt_as_is_type(type_class):
    def adaptor(type_instance):
        return AsIs(type_instance)

    register_adapter(type_class, adaptor)


adapt_as_is_type(np.int64)
adapt_as_is_type(np.int32)
adapt_as_is_type(np.float32)
adapt_as_is_type(np.float64)


def lp_ordered_array(table, spec):
    col = getattr(table, spec)
    return func.array_agg(aggregate_order_by(col, getattr(table, "cadence")))


def lp_structured_array(q, columns):
    dtypes = [(col, LIGHTPOINT_NP_DTYPES[col]) for col in columns]
    return np.array(list(map(tuple, q)), dtype=dtypes)


class LightcurveType(QLPModel, CreatedOnMixin, NameAndDescriptionMixin):
    """Describes the numerous lightcurve types"""

    __tablename__ = "lightcurvetypes"

    id = Column(SmallInteger, primary_key=True, unique=True)
    lightcurves = relationship("Lightcurve", back_populates="lightcurve_type")

    def __str__(self):
        return self.name

    def __repr__(self):
        return f"<Lightcurve Type '{self.name}'>"


class Lightcurve(QLPModel, CreatedOnMixin):
    """
    This SQLAlchemy model is used to represent the magnitude or flux
    information as a time series. Each lightcurve instance represents
    these values as a single SQL row in respect to a tic_id, lightcurve type,
    and aperture. Every lightcurve must contain a unique tuple of (tic_id,
    lightcurve type, and aperture). As of August 2020, it is expected that
    Lightcurves will contain cadence types of both 30 minutes and 10 minutes;
    with cadences numberings being repsective of each.

    ...

    Attributes
    ----------
    id : int
        The primary key identifier for tracking this lightcurve in the
        postgreSQL database. This should not be modified by hand.
    tic_id : int
        The TIC identifier for this Lightcurve. While the TIC 8 relation
        cannot be directly mapped to TIC 8 (you cannot build foreign keys
        across databases) you can assume this identifier is unique in TIC 8.
    lightcurve_type_id : str
        The lightcurve type associated with this lightcurve. It is not
        advisable to modify this attribute directly as this is a Foreign
        Key constraint.
    aperture_id : str
        The aperture associated with this lightcurve. It is not
        advisable to modify this attribute directly as this is a Foreign
        Key constraint.
    lightcurve_type: LightcurveType
        The LightcurveType model related to this lightcurve. By default
        accessing this attribute will emit an SQL query to resolve this
        model. If this access is needed in bulk or upon resolution of a query
        then as part of your query you will need:
        ::
            from sqlalchemy.orm import joinedload
            db.query(Lightcurve).options(joinedload(Lightcurve.lightcurve_type))

        This will ensure that your Lightcurve query results will already have
        their LightcurveType models already populated.
    aperture: Aperture
        The Aperture model related to this lightcurve. By default
        accessing this attribute will emit an SQL query to resolve this
        model. If this access is needed in bulk or upon resolution of a query
        then as part of your query you will need:
        ::
            from sqlalchemy.orm import joinedload
            db.query(Lightcurve).options(joinedload(Lightcurve.aperture))
        This will ensure that your Lightcurve query results will already have
        their Aperture models already populated.
    frames : list
            Not yet implemented
    cadences : np.ndarray
        A 1-Dimensional array of integers representing the all the cadence
        numberings in this lightcurve. This array will be returned in
        ascending order and must continue to be in ascending order for it
        to be accepted into the database.
    barycentric_julian_date : np.ndarray
        A 1-Dimensional array of floats representing all the barycentric
        julian dates of the lightcurve. Their ordering is directly
        related to the cadence information so the bjd[n] will be observed
        in cadences[n].
    bjd : np.ndarray
        An alias for barycentric_julian_date
    values : np.ndarray
        A 1-Dimensional array of floats representing the observed values
        of this lightcurve. The unit of these values will depend
        on the type of lightcurve. The values are ordered based upon
        the cadences of this lightcurve so values[n] will be observed in
        cadences[n]
    errors: np.ndarray
        A 1-Dimensional array of floats representing the observed errors
        of this lightcurve. The unit of these values will depend on the
        type of lightcurve. The errors are ordered based upon the cadences
        of this lightcurve so errors[n] will be observed in cadences[n]
    x_centroids : np.ndarray
        A 1-Dimensional array of floats representing the pixel X coordinate
        of this lightcurve on the related FFI and its aperture. The centroids
        are ordered based upon the cadences of this lightcurve so
        x_centroids[n] will be observed in cadences[n].
    y_centroids : np.ndarray
        A 1-Dimensional array of floats representing the pixel y coordinate
        of this lightcurve on the related FFI and its aperture. The centroids
        are ordered based upon the cadences of this lightcurve so
        y_centroids[n] will be observed in cadences[n].
    quality_flags : np.ndarray
        A 1-Dimensional array of integers representing the quality flags
        of this lightcurve. Currently the values are either 0 (OK) or
        1 (BAD). In the future this may change to utilize the remaining
        31 bits left on this field. The quality flags are ordered based upon
        the cadences of this lightcurve so quality_flags[n] will be observed
        in cadences[n].

    """

    __tablename__ = "lightcurves"
    # Constraints
    __table_args__ = (
        UniqueConstraint(
            "lightcurve_type_id",
            "aperture_id",
            "tic_id",
            name="unique_lightcurve_constraint",
        ),
    )

    id = Column(
        BigInteger,
        Sequence("lightcurves_id_seq", cache=10**6),
        primary_key=True,
    )
    tic_id = Column(BigInteger, index=True)
    cadence_type = Column(SmallInteger, index=True)

    # Foreign Keys
    lightcurve_type_id = Column(
        ForeignKey(LightcurveType.id, onupdate="CASCADE", ondelete="RESTRICT"),
        index=True,
    )
    aperture_id = Column(
        ForeignKey("apertures.name", onupdate="CASCADE", ondelete="RESTRICT"),
        index=True,
    )

    _lightpoint_cache = None

    # Relationships
    lightcurve_type = relationship(
        "LightcurveType", back_populates="lightcurves"
    )

    aperture = relationship("Aperture", back_populates="lightcurves")
    observations = relationship("Observation", back_populates="lightcurve")

    def __len__(self):
        """
        Returns
        -------
        int
            The length of the lightcurve.
        """
        return len(self.lightpoints)

    def __iter__(self):
        """
        Iterate and yield lightpoints in cadence order
        """
        return iter(self.lightpoints)

    def __repr__(self):
        return "<Lightcurve {0} {1} {2}>".format(
            self.lightcurve_type.name, self.tic_id, self.aperture.name
        )

    def __str__(self):
        return "<Lightcurve {0} {1} {2}>".format(
            self.lightcurve_type.name, self.tic_id, self.aperture.name
        )

    def __getitem__(self, key):
        """
        TODO Cleanup & move aliases to some configurable constant
        """
        key = key.lower()
        try:
            return getattr(self, key)
        except AttributeError:
            # Attempt to fallback
            if key in ("flux", "mag", "magnitude", "value"):
                return self.values
            elif key in (
                "error",
                "err",
                "fluxerr",
                "flux_err",
                "magerr",
                "mag_err",
                "magnitude_err",
                "magnitudeerror",
            ):
                return self.errors
            elif key in ("x", "y"):
                return getattr(self, "{0}_centroids".format(key))
            else:
                raise

    def __setitem__(self, key, value):
        """
        TODO Cleanup & move aliases to some configurable constant
        """
        key = key.lower()
        try:
            setattr(self, key, value)
        except AttributeError:
            if key in ("flux", "mag", "magnitude", "value"):
                self.values = value
            elif key in (
                "error",
                "err",
                "fluxerr",
                "flux_err",
                "magerr",
                "mag_err",
                "magnitude_err",
                "magnitudeerror",
            ):
                self.errors = value
            elif key in ("x", "y"):
                return setattr(self, "{0}_centroids".format(key))
            else:
                raise

    def plot(self, plot_visitor):
        raise NotImplementedError

    @property
    def to_dict(self):
        return {
            "tic_id": self.tic_id,
            "aperture": self.aperture_id,
            "type": self.lightcurve_type_id,
            "cadences": self.cadences,
            "bjd": self.bjd,
            "mag": self.values,
            "errors": self.errors,
            "x_centroids": self.x_centroids,
            "y_centroids": self.y_centroids,
            "quality_flags": self.quality_flags,
        }

    @hybrid_property
    def type(self):
        """An alias for lightcurve_type"""
        return self.lightcurve_type

    # Define lightpoint hybrid properties
    @hybrid_property
    def cadences(self):
        return self.lightpoints["cadence"]

    @hybrid_property
    def bjd(self):
        return self.lightpoints["barycentric_julian_date"]

    @hybrid_property
    def barycentric_julian_date(self):
        return self.lightpoints["barycentric_julian_date"]

    @hybrid_property
    def values(self):
        return self.lightpoints["data"]

    @hybrid_property
    def errors(self):
        return self.lightpoints["error"]

    @hybrid_property
    def x_centroids(self):
        return self.lightpoints["x_centroid"]

    @hybrid_property
    def y_centroids(self):
        return self.lightpoints["y_centroid"]

    @hybrid_property
    def quality_flags(self):
        return self.lightpoints["quality_flag"]

    # Lightcurve instance setters
    @bjd.setter
    def bjd(self, values):
        self.lightpoints["barycentric_julian_date"] = values

    @barycentric_julian_date.setter
    def barycentric_julian_date(self, values):
        self.bjd = values

    @values.setter
    def values(self, _values):
        self.lightpoints["data"] = _values

    @errors.setter
    def errors(self, values):
        self.lightpoints["error"] = values

    @x_centroids.setter
    def x_centroids(self, values):
        self.lightpoints["x_centroid"] = values

    @y_centroids.setter
    def y_centroids(self, values):
        self.lightpoints["y_centroid"] = values

    @quality_flags.setter
    def quality_flags(self, values):
        self.lightpoints["quality_flag"] = values

    @property
    def lightpoints(self):
        """
        To improve query performance while serverside statistics are being
        re-evaluated an extra -1 id query is emitted in order to force the
        query planner to use the correct index.
        """

        cols = (
            "cadence",
            "barycentric_julian_date",
            "data",
            "error",
            "x_centroid",
            "y_centroid",
            "quality_flag",
        )

        if self._lightpoint_cache is None:
            session = inspect(self).session
            q = (
                session.query(*(getattr(Lightpoint, col) for col in cols))
                .filter(Lightpoint.lightcurve_id == self.id)
                .distinct(Lightpoint.lightcurve_id, Lightpoint.cadence)
                .order_by(Lightpoint.cadence)
            )
            self._lightpoint_cache = lp_structured_array(q, cols)

        return self._lightpoint_cache

    def lightpoints_by_cadence_q(self, cadence_q):
        q = cadence_q.subquery()

        return self.lightpoint_q.filter(
            Lightpoint.cadence.between(q.c.min_cadence, q.c.max_cadence)
        )

    def update(self):
        session = inspect(self).session
        (
            session.query(Lightpoint)
            .filter_by(lightcurve_id=self.id)
            .delete(synchronize_session=False)
        )
        cols = (
            "cadence",
            "barycentric_julian_date",
            "data",
            "error",
            "x_centroid",
            "y_centroid",
            "quality_flag",
        )

        data = []
        for row in self.lightpoints:
            result = dict(zip(cols, row))
            data.append(result)

        session.bulk_insert_mappings(Lightpoint, data)


class OrbitLightcurve(QLPModel, CreatedOnMixin):
    __tablename__ = "orbit_lightcurves"

    id = Column(
        BigInteger, Sequence("orbit_lightcurve_id_seq"), primary_key=True
    )
    tic_id = Column(BigInteger)
    camera = Column(SmallInteger)
    ccd = Column(SmallInteger)
    orbit_id = Column(
        Integer,
        ForeignKey("orbits.id", onupdate="CASCADE", ondelete="CASCADE"),
    )
    aperture_id = Column(
        SmallInteger,
        ForeignKey("apertures.id", onupdate="CASCADE", ondelete="RESTRICT"),
    )
    lightcurve_type_id = Column(SmallInteger, ForeignKey("lightcurvetypes.id"))

    aperture = relationship("Aperture")
    lightcurve_type = relationship("LightcurveType")
    orbit = relationship("Orbit")

    best = relationship(
        "BestOrbitLightcurve", back_populates="orbit_lightcurve"
    )


class OrbitLightcurveAPIMixin:
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
        subq = select(
            OrbitLightcurve.id,
            (
                func.lead(OrbitLightcurve.id)
                .over(order_by=OrbitLightcurve.id)
                .label("next_id")
            ),
        ).subquery()
        q = select(
            (subq.c.id + 1).label("gap_start"),
            (subq.c.next_id - 1).label("gap_end"),
        ).where(subq.c.id + 1 != subq.c.next_id)
        return self.execute(q).fetchall()
