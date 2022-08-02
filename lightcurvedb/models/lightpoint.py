import numpy as np
import pandas as pd
from sqlalchemy import (
    BigInteger,
    Column,
    ForeignKey,
    Float,
    Index,
    Integer,
    event,
    func,
)
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION, aggregate_order_by
from sqlalchemy.ext.hybrid import hybrid_property

from lightcurvedb.core.base_model import QLPModel
from lightcurvedb.core.datastructures.blob import Blobable
from lightcurvedb.core.partitioning import (
    Partitionable,
    emit_ranged_partition_ddl,
)
from lightcurvedb.util.iter import keyword_zip

UPDATEABLE_PARAMS = [
    "barycentric_julian_date",
    "data",
    "error",
    "x_centroid",
    "y_centroid",
    "quality_flags",
]

LIGHTPOINT_ALIASES = {
    "cadences": "cadence",
    "bjd": "barycentric_julian_date",
    "values": "data",
    "value": "data",
    "mag": "data",
    "flux": "data",
    "errors": "error",
    "flux_err": "error",
    "mag_error": "error",
    "x_centroids": "x_centroid",
    "y_centroids": "y_centroid",
    "quality_flags": "quality_flag",
}

LIGHTPOINT_NP_DTYPES = {
    "lightcurve_id": "uint64",
    "cadence": "uint32",
    "barycentric_julian_date": "float32",
    "data": "float64",
    "error": "float64",
    "x_centroid": "float32",
    "y_centroid": "float32",
    "quality_flag": "uint16",
}


def get_lightpoint_dtypes(*fields):
    types = list((field, LIGHTPOINT_NP_DTYPES[field]) for field in fields)
    return np.dtype(types)


class Lightpoint(QLPModel, Blobable):
    """
    This SQLAlchemy model is used to represent individual datapoints of
    a ``Lightcurve``.
    """

    __tablename__ = "hyper_lightpoints"

    lightcurve_id = Column(
        BigInteger,
        ForeignKey("orbit_lightcurves.id", onupdate="CASCADE", ondelete="CASCADE"),
        primary_key=True
    )

    cadence = Column(Integer, primary_key=True)
    barycentric_julian_date = Column(Float, nullable=False)
    data = Column(DOUBLE_PRECISION)
    error = Column(DOUBLE_PRECISION)
    x_centroid = Column(Float)
    y_centroid = Column(Float)
    quality_flag = Column(Integer, nullable=False)

    def __repr__(self):
        return "<Lightpoint {0}-{1} {2}>".format(
            self.lightcurve_id, self.cadence, self.data
        )

    def __getitem__(self, key):
        """
        Normalize ``key`` through ``LIGHTPOINT_ALIASES`` and
        attempt to grab the relevant attribute.

        Parameters
        ----------
        key: object
            A keying object, usually a string, that was not found on the base
            lightpoint model. Attempt to normalize any aliases and try again.
        Returns
        -------
        object
            The desired attribute of Lightpoint.
        Raises
        ------
        KeyError:
            If ``key`` is not found within the lightpoint.
        """

        aliased = LIGHTPOINT_ALIASES.get(key, key)

        try:
            return getattr(self, aliased)
        except AttributeError:
            raise KeyError(
                "key {0} aliased to {1} was not found on Lightpoint".format(
                    key, aliased
                )
            )

    def __setitem__(self, key, value):
        """
        Normalize ``key`` through ``LIGHTPOINT_ALIASES`` and
        attempt to assign the relevant attribute.


        Parameters
        ----------
        key: object
            A keying object, usually a string, that was not found on the base
            lightpoint model. Attempt to normalize any aliases and try again.

        Raises
        ------
        KeyError:
            If ``key`` is not found within the lightpoint.
        """

        aliased = LIGHTPOINT_ALIASES.get(key, key)
        try:
            setattr(self, aliased, value)
        except AttributeError:
            raise KeyError(
                "key {0} aliased to {1} was not found on Lightpoint".format(
                    key, aliased
                )
            )

    @hybrid_property
    def bjd(self):
        return self.barycentric_julian_date

    @bjd.setter
    def bjd(self, value):
        self.barycentric_julian_date = value

    @bjd.expression
    def bjd(cls):
        return cls.barycentric_julian_date

    @hybrid_property
    def x(self):
        return self.x_centroid

    @x.setter
    def x(self, value):
        self.x_centroid = value

    @x.expression
    def x(cls):
        return cls.x_centroid

    @hybrid_property
    def y(self):
        return self.y_centroid

    @y.setter
    def y(self, value):
        self.y_centroid = value

    @y.expression
    def y(cls):
        return cls.y_centroid

    @classmethod
    def ordered_column(cls, column):
        col = getattr(cls, column)
        return func.array_agg(aggregate_order_by(col, cls.cadence.asc()))

    @classmethod
    def get_as_df(cls, lightcurve_ids, db):
        """
        Helper method to quickly query and return the requested
        lightcurves as a dataframe of lightpoints.

        Parameters
        ----------
        lightcurve_ids : int or iter of ints
            The ``Lightcurve.id`` to query against. If scalar is passed
            an SQL equivalency check is emitted otherwise if an
            iterable is passed an ``IN`` statement is emitted. Keep this
            in mind when your query might pass through partition ranges.

        db : lightcurvedb.DB
            The database to manage the query.
        Returns
        -------
        pd.DataFrame
            A pandas dataframe representing the lightcurves. This dataframe
            is multi-indexed by ``lightcurve_id`` and then ``cadences``.
        """
        q = db.query(
            cls.lightcurve_id,
            cls.cadence.label("cadences"),
            cls.bjd.label("barycentric_julian_date"),
            cls.data.label("values"),
            cls.error.label("errors"),
            cls.x.label("x_centroids"),
            cls.y.label("y_centroids"),
            cls.quality_flag.label("quality_flags"),
        )

        if isinstance(lightcurve_ids, int):
            # Just compare against scalar
            q = q.filter(cls.lightcurve_id == lightcurve_ids)
        else:
            # Assume iterable
            q = q.filter(cls.lightcurve_id.in_(lightcurve_ids))

        return pd.read_sql(
            q.statement,
            db.session.bind,
            index_col=["lightcurve_id", "cadences"],
        )

    # Conversion
    @property
    def to_dict(self):
        return {
            "lightcurve_id": self.lightcurve_id,
            "cadence": self.cadence,
            "barycentric_julian_date": self.bjd,
            "data": self.data,
            "error": self.error,
            "x_centroid": self.x,
            "y_centroid": self.y,
            "quality_flag": self.quality_flag,
        }

    def update_with(self, data):
        """
        Updates using the given object. The following parameters are pulled
        from the object: ``barycentric_julian_date``, ``data``, ``error``,
        ``x_centroid``, ``y_centroid``, ``quality_flag``. If these values do
        not exist within the data structure then no change is applied.
        This mean passing an empty dict() or an object that contains `none`
        of these values will have no effect.

        Parameters
        ----------
        data : any
            Data to update the lightpoint with.
        """
        for param in UPDATEABLE_PARAMS:
            try:
                new_value = getattr(data, param)
                setattr(self, param, new_value)
            except AttributeError:
                # Do not edit, fail softly
                continue
        # All edits, if any have been made
