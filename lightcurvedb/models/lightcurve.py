from lightcurvedb.core.base_model import (QLPDataProduct, QLPDataSubType,
                                          QLPModel)
from lightcurvedb.util.merge import matrix_merge
from sqlalchemy import (BigInteger, Column, ForeignKey, Integer, SmallInteger,
                        String, inspect, cast, Sequence)
from sqlalchemy.dialects.postgresql import ARRAY, DOUBLE_PRECISION
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


def addapt_int64(numpy_int64):
    return AsIs(numpy_int64)

register_adapter(np.int64, addapt_int64)


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
        backref=backref('lightcurveframemapping', cascade='all, delete-orphan'))
    frame = relationship('Frame')


class Lightcurve(QLPModel):
    """Revising lightcurve to use arrays"""
    __tablename__ = 'lightcurves'
    # Constraints
    __table_args__ = (
        UniqueConstraint('lightcurve_type_id', 'aperture_id', 'tic_id'),
    )

    id = Column(BigInteger, Sequence('lightcurve_id_seq', cache=10**6),primary_key=True)
    tic_id = Column(BigInteger, index=True)
    cadence_type = Column(SmallInteger, index=True)

    # Foreign Keys
    lightcurve_type_id = Column(ForeignKey('lightcurvetypes.id', onupdate='CASCADE', ondelete='RESTRICT'), index=True)
    aperture_id = Column(ForeignKey('apertures.id', onupdate='CASCADE', ondelete='RESTRICT'), index=True)

    # Relationships
    lightcurve_type = relationship('LightcurveType', back_populates='lightcurves')
    aperture = relationship('Aperture', back_populates='lightcurves')

    _cadences = Column('cadences', ARRAY(Integer, dimensions=1), nullable=False)
    _bjd = Column('barycentric_julian_date', ARRAY(DOUBLE_PRECISION, dimensions=1), nullable=False)
    _values = Column('values', ARRAY(DOUBLE_PRECISION, dimensions=1), nullable=False)
    _errors = Column('errors', ARRAY(DOUBLE_PRECISION, dimensions=1), nullable=False)
    _x_centroids = Column('x_centroids', ARRAY(DOUBLE_PRECISION, dimensions=1), nullable=False)
    _y_centroids = Column('y_centroids', ARRAY(DOUBLE_PRECISION, dimensions=1), nullable=False)
    _quality_flags = Column('quality_flags', ARRAY(Integer, dimensions=1), nullable=False)

    def __len__(self):
        return len(self._cadences)

    def __repr__(self):
        return '<Lightcurve {} {} {}>'.format(self.lightcurve_type.name, self.tic_id, self.aperture.name)

    def __getitem__(self, key):
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
        return self.lightcurve_type

    @hybrid_property
    def to_np(self):
        return np.array([
            self.cadences,
            self.bjd,
            self.values,
            self.errors,
            self.x_centroids,
            self.y_centroids,
            self.quality_flags
        ])

    @property
    def to_df(self):
        df = pd.DataFrame(
            index=self.cadences,
            data={
                'bjd': self._bjd,
                'values': self._values,
                'errors': self._errors,
                'x_centroids': self.x_centroids,
                'y_centroids': self.y_centroids,
                'quality_flags': self._quality_flags
                },
        )
        return df

    def merge(self, *dataframes):

        frames = [self.to_df]
        frames += dataframes

        current_data = pd.concat(frames)

        # Remove duplicates
        current_data = current_data[~current_data.index.duplicated(keep='last')]
        current_data.sort_index(inplace=True)

        self.cadences = current_data.index
        self.bjd = current_data['bjd']
        self.values = current_data['values']
        self.errors = current_data['errors']
        self.x_centroids = current_data['x_centroids']
        self.y_centroids = current_data['y_centroids']
        self.quality_flags = current_data['quality_flags']


    @hybrid_property
    def cadences(self):
        return np.array(self._cadences)

    @hybrid_property
    def bjd(self):
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


    # Foreign Keys
    lightcurve_type_id = Column(ForeignKey('lightcurvetypes.id', onupdate='CASCADE', ondelete='RESTRICT'), index=True)
    aperture_id = Column(ForeignKey('apertures.id', onupdate='CASCADE', ondelete='RESTRICT'), index=True)

    # Relationships
    lightcurve_type = relationship('LightcurveType')
    aperture = relationship('Aperture')
    frames = association_proxy(LightcurveFrameMap.__tablename__, 'frame')
