from lightcurvedb.core.base_model import QLPModel
from lightcurvedb.core.partitioning import (Partitionable,
                                            emit_ranged_partition_ddl)
import pandas as pd
from sqlalchemy import (BigInteger, Column, ForeignKey, Index, Integer, Sequence, event)
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship


LIGHTPOINT_PARTITION_RANGE = 10**6
UPDATEABLE_PARAMS = [
    'barycentric_julian_date',
    'data',
    'error',
    'x_centroid',
    'y_centroid',
    'quality_flags'
]


class Lightpoint(QLPModel, Partitionable('range', 'lightcurve_id')):
    """
    This SQLAlchemy model is used to represent individual datapoints of
    a ``Lightcurve``.
    """
    __tablename__ = 'lightpoints'
    __abstract__ = False

    lightcurve = relationship(
        'Lightcurve',
        back_populates='lightpoints'
    )

    lightcurve_id = Column(
        ForeignKey(
            'lightcurves.id',
            onupdate='CASCADE',
            ondelete='CASCADE'
        ),
        primary_key=True,
        index=Index(
            'ix_lightpoints_lightcurve_id',
            'lightpoint_id',
            postgresql_using='brin'
        ),
        nullable=False
    )

    cadence = Column(
        BigInteger,
        nullable=False,
        primary_key=True,
        index=Index(
            'lightpoints_cadence_idx',
            'cadence',
            postgresql_using='brin',
            postgresql_concurrently=True
        )
    )

    barycentric_julian_date = Column(
        DOUBLE_PRECISION,
        nullable=False
    )

    data = Column(
        DOUBLE_PRECISION
    )

    error = Column(
        DOUBLE_PRECISION
    )

    x_centroid = Column(
        DOUBLE_PRECISION
    )

    y_centroid = Column(
        DOUBLE_PRECISION
    )

    quality_flag = Column(
        Integer,
        nullable=False
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
            cls.cadence.label('cadences'),
            cls.bjd.label('barycentric_julian_date'),
            cls.data.label('values'),
            cls.error.label('errors'),
            cls.x.label('x_centroids'),
            cls.y.label('y_centroids'),
            cls.quality_flag.label('quality_flags')
        )

        if isinstance(lightcurve_ids, int):
            # Just compare against scalar
            q = q.filter(
                cls.lightcurve_id == lightcurve_ids
            )
        else:
            # Assume iterable
            q = q.filter(
                cls.lightcurve_id.in_(lightcurve_ids)
            )

        return pd.read_sql(
            q.statement,
            db.session.bind,
            index_col=['lightcurve_id', 'cadences']
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
            quality_flag=self.quality_flag
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



# Setup initial lightpoint Partition
event.listen(
    Lightpoint.__table__,
    'after_create',
    emit_ranged_partition_ddl(
        Lightpoint.__tablename__,
        0,
        LIGHTPOINT_PARTITION_RANGE
    )
)
