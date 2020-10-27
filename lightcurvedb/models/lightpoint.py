import pandas as pd

from lightcurvedb.core.base_model import QLPModel
from lightcurvedb.core.partitioning import (
    Partitionable,
    emit_ranged_partition_ddl,
)
from lightcurvedb.util.iter import keyword_zip
from sqlalchemy import (
    BigInteger,
    Column,
    ForeignKey,
    Index,
    Integer,
    event,
    func,
)
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION, aggregate_order_by
from sqlalchemy.ext.hybrid import hybrid_property

LIGHTPOINT_PARTITION_RANGE = 10 ** 6
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
    "quality_flags": "quality_flag"
}


class Lightpoint(QLPModel, Partitionable("range", "lightcurve_id")):
    """
    This SQLAlchemy model is used to represent individual datapoints of
    a ``Lightcurve``.
    """

    __tablename__ = "lightpoints"
    __abstract__ = False

    lightcurve_id = Column(
        ForeignKey("lightcurves.id", onupdate="CASCADE", ondelete="CASCADE"),
        primary_key=True,
        index=Index(
            "ix_lightpoints_lightcurve_id",
            "lightpoint_id",
            postgresql_using="brin",
        ),
        nullable=False,
    )

    cadence = Column(
        BigInteger,
        nullable=False,
        primary_key=True,
        index=Index(
            "lightpoints_cadence_idx",
            "cadence",
            postgresql_using="brin",
            postgresql_concurrently=True,
        ),
    )

    barycentric_julian_date = Column(DOUBLE_PRECISION, nullable=False)

    data = Column(DOUBLE_PRECISION)

    error = Column(DOUBLE_PRECISION)

    x_centroid = Column(DOUBLE_PRECISION)

    y_centroid = Column(DOUBLE_PRECISION)

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
        #if aliased == "cadence":
        #    return self.cadence
        #elif aliased == "barycentric_julian_date":
        #    return self.barycentric_julian_date
        #elif aliased == "data":
        #    return self.data
        #elif aliased == "error":
        #    return self.error
        #elif aliased == "x_centroid":
        #    return self.x_centroid
        #elif aliased == "y_centroid":
        #    return self.y_centroid
        #elif aliased == "quality_flag":
        #    return self.quality_flag
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
        return dict(
            lightcurve_id=self.lightcurve_id,
            cadence=self.cadence,
            barycentric_julian_date=self.bjd,
            data=self.data,
            error=self.error,
            x_centroid=self.x,
            y_centroid=self.y,
            quality_flag=self.quality_flag,
        )

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


# Define some factories
def lightpoints_from_kw(cadences, bjd, **other_data):
    """
    A factory method to construct raw Lightpoints from keyword data
    comprising of lists.

    Parameters
    ----------
    cadences : list or list-like of integers
        The cadences to align the given data. The cadence array need not be
        in ascending or descending order. However the ``bjd`` and other
        data columns will be interpreted in the order of the cadences given.

    bjd : list or list-like of floats
        The barycentric julian dates to assign to lightpoint instances. Will
        be interpreted in order of the given cadences

    **other_data : optional keyword arg of additional list-like data.
        Other keyword data to assign to lightpoints. Must have the same
        lengths as the given cadences.

    Yields
    ------
    Lightpoint
        A lightpoint instance instantiated with a column of the given
        data.
    Raises
    ------
    ValueError:
        Raised if the given data does not agree in length.
    """
    if not len(bjd) == len(cadences):
        raise ValueError(
            "bjd length {0} does not match cadence length {1}".format(
                len(bjd), len(cadences)
            )
        )
    data_keys = other_data.keys()
    for col in data_keys:
        if not len(other_data[col]) == len(cadences):
            raise ValueError(
                "{0} length {1} does not match cadence length {2}".format(
                    col, len(other_data[col]), len(cadences)
                )
            )
    # Everything is aligned
    # iterate through columnwise
    for kw in keyword_zip(cadence=cadences, bjd=bjd, **other_data):
        yield Lightpoint(**kw)


# Setup initial lightpoint Partition
event.listen(
    Lightpoint.__table__,
    "after_create",
    emit_ranged_partition_ddl(
        Lightpoint.__tablename__, 0, LIGHTPOINT_PARTITION_RANGE
    ),
)
