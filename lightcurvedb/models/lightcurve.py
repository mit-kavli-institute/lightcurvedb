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
                        SmallInteger, String, cast, inspect, join, select)
from sqlalchemy.dialects.postgresql import ARRAY, DOUBLE_PRECISION, insert
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import backref, relationship
from sqlalchemy.orm.collections import collection
from sqlalchemy.schema import UniqueConstraint
from sqlalchemy.sql import func
from sqlalchemy.sql.expression import bindparam

from lightcurvedb.core.base_model import (QLPDataProduct, QLPDataSubType,
                                          QLPModel)
from lightcurvedb.util.merge import merge_arrays


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


class Lightcurve(QLPModel):
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

    # Variables marked with '_' prefix are internal and
    # should not be modified directly

    _cadences = Column(
        'cadences',
        ARRAY(Integer, dimensions=1),
        nullable=False
    )
    _bjd = Column(
        'barycentric_julian_date',
        ARRAY(DOUBLE_PRECISION, dimensions=1),
        nullable=False
    )
    _values = Column(
        'values',
        ARRAY(DOUBLE_PRECISION, dimensions=1),
        nullable=False
    )
    _errors = Column(
        'errors',
        ARRAY(DOUBLE_PRECISION, dimensions=1),
        nullable=False
    )
    _x_centroids = Column(
        'x_centroids',
        ARRAY(DOUBLE_PRECISION, dimensions=1),
        nullable=False
    )
    _y_centroids = Column(
        'y_centroids',
        ARRAY(DOUBLE_PRECISION, dimensions=1),
        nullable=False
    )
    _quality_flags = Column(
        'quality_flags',
        ARRAY(Integer, dimensions=1),
        nullable=False
    )

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
        return np.array(self._cadences)

    @hybrid_property
    def bjd(self):
        return np.array(self._bjd)

    @hybrid_property
    def barycentric_julian_date(self):
        return np.array(self._bjd)

    @hybrid_property
    def values(self):
        return np.array(self._values)

    @hybrid_property
    def errors(self):
        return np.array(self._errors)

    @hybrid_property
    def x_centroids(self):
        return np.array(self._x_centroids)

    @hybrid_property
    def y_centroids(self):
        return np.array(self._y_centroids)

    @hybrid_property
    def quality_flags(self):
        return np.array(self._quality_flags)

    # Setters
    @cadences.setter
    def cadences(self, value):
        if isinstance(value, np.ndarray):
            value = value.tolist()
        self._cadences = value

    @bjd.setter
    def bjd(self, value):
        if isinstance(value, np.ndarray):
            value = value.tolist()
        self._bjd = value

    @barycentric_julian_date.setter
    def barycentric_julian_date(self, value):
        self._bjd = value

    @values.setter
    def values(self, value):
        if isinstance(value, np.ndarray):
            value = value.tolist()
        self._values = value

    @errors.setter
    def errors(self, value):
        if isinstance(value, np.ndarray):
            value = value.tolist()
        self._errors = value

    @x_centroids.setter
    def x_centroids(self, value):
        if isinstance(value, np.ndarray):
            value = value.tolist()
        self._x_centroids = value

    @y_centroids.setter
    def y_centroids(self, value):
        if isinstance(value, np.ndarray):
            value = value.tolist()
        self._y_centroids = value

    @quality_flags.setter
    def quality_flags(self, value):
        if isinstance(value, np.ndarray):
            value = value.tolist()
        self._quality_flags = value

    # Expressions
    @cadences.expression
    def cadences(cls):
        return cls._cadences

    @barycentric_julian_date.expression
    def barycentric_julian_date(cls):
        return cls._bjd

    @bjd.expression
    def bjd(cls):
        return cls._bjd

    @values.expression
    def values(cls):
        return cls._values

    @errors.expression
    def errors(cls):
        return cls._errors

    @x_centroids.expression
    def x_centroids(cls):
        return cls._x_centroids

    @y_centroids.expression
    def y_centroids(cls):
        return cls._y_centroids

    @quality_flags.expression
    def quality_flags(cls):
        return cls._quality_flags

    @classmethod
    def create_mappings(cls, **mappings):
        mapping = {}
        for field, binding in mappings.items():
            if binding is None:
                # If binding is None, do not map to the field
                continue
            mapper_field = '{}'.format(field)
            binding = bindparam(binding)
            mapping[mapper_field] = binding

        return mapping

    @classmethod
    def insert_with(cls, stmt):
        stmt = insert(cls).from_select(
            [cls.tic_id, cls.aperture_id, cls.lightcurve_type_id,
             cls.cadences, cls.barycentric_julian_date, cls.values,
             cls.errors, cls.x_centroids, cls.y_centroids,
             cls.quality_flags
            ],
            stmt
        )
        return stmt

    @classmethod
    def update_with(cls, cte):
        T = cls.__table__
        q = T.update().values({
            cls.cadences: cte.c.cadences,
            cls.barycentric_julian_date: cte.c.bjd,
            cls.values: cte.c._values,
            cls.errors: cte.c.errors,
            cls.x_centroids: cte.c.x_centroids,
            cls.y_centroids: cte.c.y_centroids,
            cls.quality_flags: cte.c.quality_flags
        }).where(
            cls.id == cte.c.lightcurve_id
        )

        return q
