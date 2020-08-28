"""
lightcurve.py
=============
The lightcurve model module containing the Lightcurve model class
and directly related models
"""

import numpy as np
import pandas as pd

from lightcurvedb.core.base_model import (QLPDataProduct, QLPDataSubType,
                                          QLPModel)
from lightcurvedb.core.partitioning import (Partitionable,
                                            emit_ranged_partition_ddl)
from lightcurvedb.core.datastructures.lightpoint_collection import MassTrackedLightpoints
from lightcurvedb.util.merge import merge_arrays
from psycopg2.extensions import AsIs, register_adapter
from sqlalchemy import (DDL, BigInteger, Column, ForeignKey, Index, Integer,
                        Sequence, SmallInteger, cast, event, inspect, join,
                        select)
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION, insert
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import backref, relationship
from sqlalchemy.schema import UniqueConstraint
from sqlalchemy.sql import func
from sqlalchemy.sql.expression import bindparam


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
    lightpoints = relationship(
        'Lightpoint',
        backref='lightcurve',
        collection_class=MassTrackedLightpoints)
    aperture = relationship('Aperture', back_populates='lightcurves')
    frames = association_proxy(LightcurveFrameMap.__tablename__, 'frame')

    def __len__(self):
        """
        Returns
        -------
        int
            The length of the lightcurve.
        """
        return len(self.lightpoints)

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

    # Define lightpoint hybrid properties

    @hybrid_property
    def cadences(self):
        return self.lightpoints.cadences

    @hybrid_property
    def bjd(self):
        return self.lightpoints.bjd

    @hybrid_property
    def barycentric_julian_date(self):
        return self.lightpoints.bjd

    @hybrid_property
    def values(self):
        return self.lightpoints.values

    @hybrid_property
    def errors(self):
        return self.lightpoints.errors

    @hybrid_property
    def x_centroids(self):
        return self.lightpoints.x_centroids

    @hybrid_property
    def y_centroids(self):
        return self.lightpoints.y_centroids

    @hybrid_property
    def quality_flags(self):
        return self.lightpoints.quality_flags


    # Lightcurve instance setters
    @bjd.setter
    def bjd(self, values):
        self.lightpoints.bjd = values

    @barycentric_julian_date.setter
    def barycentric_julian_date(self, values):
        self.bjd = values

    @values.setter
    def values(self, _values):
        self.lightpoints.values = _values

    @errors.setter
    def errors(self, values):
        self.lightpoints.errors = values

    @x_centroids.setter
    def x_centroids(self, values):
        self.lightpoints.x_centroids = values

    @y_centroids.setter
    def y_centroids(self, values):
        self.lightpoints.y_centroids = values

    @quality_flags.setter
    def quality_flags(self, values):
        self.lightpoints.quality_flags = values
