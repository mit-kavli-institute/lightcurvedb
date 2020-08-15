"""
lightcurve.py
=============
The lightcurve model module containing the Lightcurve model class
and directly related models
"""

import numpy as np
import pandas as pd
from psycopg2.extensions import AsIs, register_adapter
from sqlalchemy import (BigInteger, Column, ForeignKey, Integer, Sequence,
                        SmallInteger, Index, cast, inspect, join, select,
                        DDL, event)
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION, insert
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import backref, relationship
from sqlalchemy.schema import UniqueConstraint
from sqlalchemy.sql import func
from sqlalchemy.sql.expression import bindparam

from lightcurvedb.core.base_model import (QLPDataProduct, QLPDataSubType,
                                          QLPModel)
from lightcurvedb.util.merge import merge_arrays
from lightcurvedb.core.partitioning import Partitionable, emit_ranged_partition_ddl


LIGHTPOINT_PARTITION_RANGE = 10**6


def adapt_as_is_type(type_class):
    def adaptor(type_instance):
        return AsIs(type_instance)
    register_adapter(type_class, adaptor)


adapt_as_is_type(np.int64)
adapt_as_is_type(np.int32)
adapt_as_is_type(np.float32)
adapt_as_is_type(np.float64)


class LightcurveType(QLPDataSubType):
    """Describes the numerous lightcurve types"""
    __tablename__ = 'lightcurvetypes'

    lightcurves = relationship('Lightcurve', back_populates='lightcurve_type')

    def __str__(self):
        return self.name

    def __repr__(self):
        return '<Lightcurve Type {} >'.format(self.name)


class LightcurveFrameMap(QLPModel):
    __tablename__ = 'lightcurveframemapping'
    lightcurve_type_id = Column(
        ForeignKey('lightcurves.id', ondelete='CASCADE'),
        primary_key=True,
    )
    frame_id = Column(
        ForeignKey('frames.id'),
        primary_key=True,
    )

    lightcurve = relationship(
        'Lightcurve',
        backref=backref(
            'lightcurveframemapping',
            cascade='all, delete-orphan'
        )
    )
    frame = relationship('Frame')


class Lightcurve(QLPDataProduct):
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
    cadence_type : int
        Deprecated. Lightcurves will have mixed cadences starting with the
        reduction of Sector 27 (End of July 2020).
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
    __tablename__ = 'lightcurves'
    # Constraints
    __table_args__ = (
        UniqueConstraint(
            'lightcurve_type_id',
            'aperture_id',
            'tic_id',
            name='unique_lightcurve_constraint'
        ),
    )

    id = Column(
        BigInteger,
        Sequence('lightcurves_id_seq', cache=10**6),
        primary_key=True
    )
    tic_id = Column(BigInteger, index=True)
    cadence_type = Column(SmallInteger, index=True)

    # Foreign Keys
    lightcurve_type_id = Column(
        ForeignKey(
            'lightcurvetypes.name',
            onupdate='CASCADE',
            ondelete='RESTRICT'
        ),
        index=True
    )
    aperture_id = Column(
        ForeignKey(
            'apertures.name',
            onupdate='CASCADE',
            ondelete='RESTRICT'
        ),
        index=True
    )

    # Relationships
    lightcurve_type = relationship(
        'LightcurveType',
        back_populates='lightcurves'
    )
    aperture = relationship('Aperture', back_populates='lightcurves')
    frames = association_proxy(LightcurveFrameMap.__tablename__, 'frame')
    
    def __len__(self):
        """
        Returns
        -------
        int
            The length of the lightcurve. Since cadences are the base
            reference for all time-series fields, specifically the length
            of the cadences is returned.
        """
        return len(self._cadences)

    def __repr__(self):
        return '<Lightcurve {} {} {}>'.format(
            self.lightcurve_type.name,
            self.tic_id,
            self.aperture.name
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
            if key in ('flux', 'mag', 'magnitude', 'value'):
                return self.values
            elif key in ('error', 'err', 'fluxerr', 'flux_err', 'magerr', 'mag_err', 'magnitude_err', 'magnitudeerror'):
                return self.errors
            elif key in ('x', 'y'):
                return getattr(self, '{}_centroids'.format(key))
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
            if key in ('flux', 'mag', 'magnitude', 'value'):
                self.values = value
            elif key in ('error', 'err', 'fluxerr', 'flux_err', 'magerr', 'mag_err', 'magnitude_err', 'magnitudeerror'):
                self.errors = value
            elif key in ('x', 'y'):
                return setattr(self, '{}_centroids'.format(key))
            else:
                raise

    @hybrid_property
    def type(self):
        """An alias for lightcurve_type"""
        return self.lightcurve_type

    @property
    def to_df(self):
        """
        Convert this lightcurve into a pandas dataframe
        Returns
        -------
        pd.DataFrame
        """
        df = pd.DataFrame(
            index=self.cadences,
            data={
                'barycentric_julian_date': self._bjd,
                'values': self._values,
                'errors': self._errors,
                'x_centroids': self.x_centroids,
                'y_centroids': self.y_centroids,
                'quality_flags': self._quality_flags
                },
        )
        return df

    @property
    def to_dict(self):
        """
        Represent this lightcurve as a dictionary
        Returns
        -------
        dict
        """
        return dict(
            id=self.id,
            tic_id=self.tic_id,
            aperture_id=self.aperture_id,
            lightcurve_type_id=self.lightcurve_type_id,
            cadences=self.cadences,
            barycentric_julian_date=self.bjd,
            values=self.values,
            errors=self.errors,
            x_centroids=self.x_centroids,
            y_centroids=self.y_centroids,
            quality_flags=self.quality_flags
        )

    def merge_df(self, *dataframes):
        """
        Merge the current lightcurve with the given Lightpoint dataframes.
        This merge will handle all cadence orderings and duplicate entries
        """
        frames = [self.to_df]
        frames += dataframes

        current_data = pd.concat(frames)

        # Remove duplicates
        current_data = current_data[
            ~current_data.index.duplicated(keep='last')
        ]
        current_data.sort_index(inplace=True)

        self.cadences = current_data.index
        self.bjd = current_data['barycentric_julian_date']
        self.values = current_data['values']
        self.errors = current_data['errors']
        self.x_centroids = current_data['x_centroids']
        self.y_centroids = current_data['y_centroids']
        self.quality_flags = current_data['quality_flags']

        return self

    def merge_np(
            self,
            cadences,
            bjd,
            values,
            errors,
            x_centroids,
            y_centroids,
            quality_flags):

        raw_cadences = np.concatenate((self.cadences, cadences))
        raw_bjd = np.concatenate((self.bjd, bjd))
        raw_values = np.concatenate((self.values, values))
        raw_errors = np.concatenate((self.errors, errors))
        raw_x = np.concatenate((self.x_centroids, x_centroids))
        raw_y = np.concatenate((self.y_centroids, y_centroids))
        raw_qflag = np.concatenate((self.quality_flags, quality_flags))

        # Determine sort and diff of cadences
        merged_cadences, merged_data = merge_arrays(
            raw_cadences,
            bjd=raw_bjd,
            values=raw_values,
            errors=raw_errors,
            x_centroids=raw_x,
            y_centroids=raw_y,
            quality_flags=raw_qflag
        )

        self.cadences = merged_cadences
        self.bjd = merged_data['bjd']
        self.values = merged_data['values']
        self.errors = merged_data['errors']
        self.x_centroids = merged_data['x_centroids']
        self.y_centroids = merged_data['y_centroids']
        self.quality_flags = merged_data['quality_flags']

        return self

    def merge(self, other_lc):
        if self.id != other_lc.id:
            raise ValueError(
                '{} does not have the same ID as {}, cannot merge'.format(
                    self,
                    other_lc
                )
            )
        self.merge_df(other_lc.to_df)

    @hybrid_property
    def cadences(self):
        return [lp.cadence for lp in self.lightpoints]

    @hybrid_property
    def bjd(self):
        return [lp.bjd for lp in self.lightpoints]

    @hybrid_property
    def barycentric_julian_date(self):
        return [lp.bjd for lp in self.lightpoints]

    @hybrid_property
    def values(self):
        return [lp.data for lp in self.lightpoints]

    @hybrid_property
    def errors(self):
        return [lp.error for lp in self.lightpoints]

    @hybrid_property
    def x_centroids(self):
        return [lp.x for lp in self.lightpoints]

    @hybrid_property
    def y_centroids(self):
        return [lp.y for lp in self.lightpoints]

    @hybrid_property
    def quality_flags(self):
        return [lp.quality_flag for lp in self.lightpoints]


class Lightpoint(QLPModel, Partitionable('range', 'lightcurve_id')):
    __tablename__ = 'lightpoints'
    __abstract__ = False

    lightcurve = relationship(
        'Lightcurve',
        backref='lightpoints'
    )

    lightcurve_id = Column(
        ForeignKey(
            'lightcurves.id',
            onupdate='CASCADE',
            ondelete='CASCADE'
        ),
        primary_key=True,
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
