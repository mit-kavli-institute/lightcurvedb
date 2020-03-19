from lightcurvedb.core.base_model import (QLPDataProduct, QLPDataSubType,
                                          QLPModel)
from lightcurvedb.util.merge import matrix_merge
from sqlalchemy import (BigInteger, Column, ForeignKey, Integer, SmallInteger,
                        String, inspect, cast)
from sqlalchemy.dialects.postgresql import ARRAY, DOUBLE_PRECISION
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import backref, relationship
from sqlalchemy.orm.collections import collection
from sqlalchemy.schema import CheckConstraint, UniqueConstraint
from sqlalchemy.sql import select, func
import numpy as np

from .lightpoint import LOOKUP, Lightpoint


class LightpointsMap(object):
    __emulates__ = list

    def __init__(self):
        self.data = []

    @collection.appender
    def append_lightpoint(self, lightpoint):
        self.data.append(lightpoint)


class LightcurveType(QLPDataSubType):
    """Describes the numerous lightcurve types"""
    __tablename__ = 'lightcurvetypes'

    lightcurves = relationship('Lightcurve', back_populates='lightcurve_type')


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


class Lightcurve(QLPDataProduct):
    """
        Provides ORM implementation of a lightcurve used by QLP

        These lightcurves are per orbit and are returned in batches
    """

    __tablename__ = 'lightcurves'

    # Constraints
    __table_args__ = (
        UniqueConstraint('cadence_type', 'lightcurve_type_id', 'aperture_id', 'tic_id'),
    )

    tic_id = Column(BigInteger, index=True)
    cadence_type = Column(SmallInteger, index=True)

    # Foreign Keys
    lightcurve_type_id = Column(ForeignKey('lightcurvetypes.id', onupdate='CASCADE', ondelete='RESTRICT'), index=True)
    aperture_id = Column(ForeignKey('apertures.id', onupdate='CASCADE', ondelete='RESTRICT'), index=True)

    # Relationships
    lightcurve_type = relationship('LightcurveType', back_populates='lightcurves')
    lightpoints = relationship(
        'Lightpoint',
        back_populates='lightcurve',
        order_by='Lightpoint.cadence')
    aperture = relationship('Aperture', back_populates='lightcurves')
    frames = association_proxy(LightcurveFrameMap.__tablename__, 'frame')

    def __init__(self, *args, **kwargs):
        super(Lightcurve, self).__init__(*args, **kwargs)

        # We want to cache lightpoint attributes to avoid hitting
        # the database with expensive queries
        self._lightpoint_cache = {}

    def __len__(self):
        return self.length

    def _get_attr_array(self, attr):
        q = select(
            [attr]
        ).where(
            Lightpoint.lightcurve_id == self.id
        )
        session = inspect(self).session
        return list(session.execute(q))

    def _get_from_cache(self, attr):
        if not attr in self._lightpoint_cache:
            col = LOOKUP[attr]
            attr_v = self._get_attr_array(col)
            self._lightpoint_cache[attr] = attr_v
        return self._lightpoint_cache[attr]

    @hybrid_property
    def length(self):
        return len(self.lightpoints)

    @length.expression
    def length(self):
        return select(
            [func.count(Lightpoint.id)]
        ).correlate(cls).where(Lightpoint.lightcurve_id == cls.id).label('length')

    @hybrid_property
    def max_cadence(self):
        return self.lightpoints[-1].cadence

    @max_cadence.expression
    def max_cadence(cls):
        return select(
            [func.max(Lightpoint.cadence)]
        ).correlate(cls).where(Lightpoint.lightcurve_id == cls.id).label('max_cadence')

    @hybrid_property
    def min_cadence(self):
        return min(lp.cadence for lp in self.lightpoints)

    @min_cadence.expression
    def min_cadence(cls):
        return select(
            [func.min(Lightpoint.cadence)]
        ).correlate(cls).where(Lightpoint.lightcurve_id == cls.id).label('min_cadence')

    # Getters
    @hybrid_property
    def cadences(self):
        return [lp.cadence for lp in self.lightpoints]

    @cadences.expression
    def cadences(cls):
        return select([Lightpoint.cadence]).where(
            Lightpoint.lightcurve_id == cls.id
        ).as_scalar()

    @hybrid_property
    def bjd(self):
        return [lp.barycentric_julian_date for lp in self.lightpoints]

    @hybrid_property
    def values(self):
        return [lp.value for lp in self.lightpoints]

    @hybrid_property
    def errors(self):
        return [lp.error for lp in self.lightpoints]

    @hybrid_property
    def x_centroids(self):
        return [lp.x_centroid for lp in self.lightpoints]

    @hybrid_property
    def y_centroids(self):
        return [lp.y_centroid for lp in self.lightpoints]

    @hybrid_property
    def quality_flags(self):
        return [lp.quality_flag for lp in self.lightpoints]

    # Setters
    @cadences.update_expression
    def cadences(self, value):
        raise NotImplemented

    def __repr__(self):
        return '<Lightcurve {} {}>'.format(
            self.tic_id,
            self.lightcurve_type.name)


class LightcurveRevision(QLPDataProduct):
    """Revising lightcurve to use arrays"""
    __tablename__ = 'lightcurve_revisions'
    # Constraints
    __table_args__ = (
        UniqueConstraint('cadence_type', 'lightcurve_type_id', 'aperture_id', 'tic_id'),
    )

    tic_id = Column(BigInteger, index=True)
    cadence_type = Column(SmallInteger, index=True)

    _cadences = Column('cadences', ARRAY(Integer, dimensions=1), nullable=False)
    _bjd = Column('barycentric_julian_date', ARRAY(Integer, dimensions=1), nullable=False)
    _values = Column('values', ARRAY(DOUBLE_PRECISION, dimensions=1), nullable=False)
    _errors = Column('errors', ARRAY(DOUBLE_PRECISION, dimensions=1), nullable=False)
    _x_centroids = Column('x_centroids', ARRAY(DOUBLE_PRECISION, dimensions=1), nullable=False)
    _y_centroids = Column('y_centroids', ARRAY(DOUBLE_PRECISION, dimensions=1), nullable=False)
    _quality_flags = Column('quality_flags', ARRAY(Integer, dimensions=1), nullable=False)

    def __len__(self):
        return len(self._cadences)

    def __repr__(self):
        return '<Lightcurve {} {} {}>'.format(self.lightcurve_type.name, self.tic_id, self.aperture.name)

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

    def merge(self, *data):
        compiled = matrix_merge(self.to_np, *data)
        self.cadences = compiled[0]
        self.bjd = compiled[1]
        self.values = compiled[2]
        self.errors = compiled[3]
        self.x_centroids = compiled[4]
        self.y_centroids = compiled[5]
        self.quality_flags = compiled[6]


    @hybrid_property
    def cadences(self):
        return self._cadences

    @hybrid_property
    def bjd(self):
        return self._bjd

    @hybrid_property
    def values(self):
        return self._values

    @hybrid_property
    def errors(self):
        return self._errors

    @hybrid_property
    def x_centroids(self):
        return self._x_centroids

    @hybrid_property
    def y_centroids(self):
        return self._y_centroids

    @hybrid_property
    def quality_flags(self):
        return self._quality_flags

    # Setters
    @cadences.setter
    def cadences(self, value):
        self._cadences = value

    @bjd.setter
    def bjd(self, value):
        self._bjd = value

    @values.setter
    def values(self, value):
        self._values = value

    @errors.setter
    def errors(self, value):
        self._errors = value

    @x_centroids.setter
    def x_centroids(self, value):
        self._x_centroids = value

    @y_centroids.setter
    def y_centroids(self, value):
        self._y_centroids = value

    @quality_flags.setter
    def quality_flags(self, value):
        self._quality_flags = value


    # Foreign Keys
    lightcurve_type_id = Column(ForeignKey('lightcurvetypes.id', onupdate='CASCADE', ondelete='RESTRICT'), index=True)
    aperture_id = Column(ForeignKey('apertures.id', onupdate='CASCADE', ondelete='RESTRICT'), index=True)

    # Relationships
    lightcurve_type = relationship('LightcurveType')
    aperture = relationship('Aperture')
