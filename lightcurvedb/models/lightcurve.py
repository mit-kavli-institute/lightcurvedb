from lightcurvedb.core.base_model import (QLPDataProduct, QLPDataSubType,
                                          QLPModel)
from lightcurvedb.util.merge import matrix_merge
from sqlalchemy import (BigInteger, Column, ForeignKey, Integer, SmallInteger,
                        String, inspect, cast, Sequence, select, join)
from sqlalchemy.dialects.postgresql import ARRAY, DOUBLE_PRECISION, insert
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import backref, relationship
from sqlalchemy.orm.collections import collection
from sqlalchemy.schema import CheckConstraint, UniqueConstraint
from sqlalchemy.sql import select, func
from sqlalchemy.sql.expression import bindparam
from psycopg2.extensions import AsIs, register_adapter
import numpy as np
import pandas as pd



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
    """Revising lightcurve to use arrays"""
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
        """Return the type of lightcurve for this paricular instance"""
        return self.lightcurve_type

    @property
    def to_df(self):
        """Conver this lightcurve into a pandas dataframe"""
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
        path = np.argsort(raw_cadences)
        check = np.append(
            np.diff(raw_cadences[path]),
            1  # Always keep last element
        )

        self.cadences = raw_cadences[check]
        self.bjd = raw_bjd[check]
        self.values = raw_values[check]
        self.errors = raw_errors[check]
        self.x_centroids = raw_x[check]
        self.y_centroids = raw_y[check]
        self.quality_flags = raw_qflag[check]

        return self

    def merge(self, other_lc):
        if self.id != other_lc.id:
            raise ValueError(
                '{} does not have the same ID as {}, cannot merge'.format(
                    self,
                    other_lc
                )
            )
        self.merge_np(
            other_lc.cadences,
            other_lc.bjd,
            other_lc.values,
            other_lc.errors,
            other_lc.x_centroids,
            other_lc.y_centroids,
            other_lc.quality_flags
        )

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
