from sqlalchemy import Column, Integer, String, SmallInteger, ForeignKey, BigInteger
from sqlalchemy.orm import relationship, backref
from sqlalchemy.schema import UniqueConstraint, CheckConstraint
from sqlalchemy.dialects.postgresql import ARRAY, DOUBLE_PRECISION
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.ext.associationproxy import association_proxy
from lightcurvedb.core.base_model import QLPDataProduct, QLPModel, QLPDataSubType


def array_congruence(target_array_field, cmp_field='cadences'):
    return CheckConstraint(
        'array_length({}, 1) = array_length({}, 1)'.format(target_array_field, cmp_field),
        name='{}_congruence'.format(target_array_field)
    )


class LightcurveType(QLPDataSubType):
    """Describes the numerous lightcurve types"""
    __tablename__ = 'lightcurvetypes'

    lightcurves = relationship('OrbitLightcurve', back_populates='lightcurve_type')


class LightcurveFrameMap(QLPModel):
    __tablename__ = 'lightcurveframemapping'
    lightcurve_type_id = Column(
        ForeignKey('orbitlightcurves.id', ondelete='CASCADE'),
        primary_key=True,
    )
    frame_id = Column(
        ForeignKey('frames.id'),
        primary_key=True,
    )

    lightcurve = relationship(
        'OrbitLightcurve',
        backref=backref('lightcurveframemapping', cascade='all, delete-orphan'))
    frame = relationship('Frame')


class OrbitLightcurve(QLPDataProduct):
    """
        Provides ORM implementation of a lightcurve used by QLP

        These lightcurves are per orbit and are returned in batches
    """

    __tablename__ = 'orbitlightcurves'

    # Constraints
    __table_args__ = (
        UniqueConstraint('cadence_type', 'lightcurve_type_id', 'aperture_id', 'tic_id'),
        array_congruence('barycentric_julian_date'),
        array_congruence('flux'),
        array_congruence('flux_err'),
        array_congruence('x_centroids'),
        array_congruence('y_centroids'),
        array_congruence('meta'),
    )

    tic_id = Column(BigInteger, index=True)
    cadence_type = Column(SmallInteger, index=True)

    _cadences = Column('cadences', ARRAY(Integer, dimensions=1), nullable=False)
    _bjd = Column('barycentric_julian_date', ARRAY(Integer, dimensions=1), nullable=False)
    _flux = Column('flux', ARRAY(DOUBLE_PRECISION, dimensions=1), nullable=False)
    _flux_err = Column('flux_err', ARRAY(DOUBLE_PRECISION, dimensions=1), nullable=False)
    _x_centroids = Column('x_centroids', ARRAY(DOUBLE_PRECISION, dimensions=1), nullable=False)
    _y_centroids = Column('y_centroids', ARRAY(DOUBLE_PRECISION, dimensions=1), nullable=False)
    _meta = Column('meta', ARRAY(Integer, dimensions=1), nullable=False)
    
    # Foreign Keys
    lightcurve_type_id = Column(ForeignKey('lightcurvetypes.id', onupdate='CASCADE', ondelete='RESTRICT'), index=True)
    aperture_id = Column(ForeignKey('apertures.id', onupdate='CASCADE', ondelete='RESTRICT'), index=True)
    orbit_id = Column(ForeignKey('orbits.id', onupdate='CASCADE', ondelete='RESTRICT'), index=True)

    # Relationships
    lightcurve_type = relationship('LightcurveType', back_populates='lightcurves')
    aperture = relationship('Aperture', back_populates='lightcurves')
    frames = association_proxy(LightcurveFrameMap.__tablename__, 'frame')
    orbit = relationship('Orbit', back_populates='lightcurves')

    def __repr__(self):
        return '<Lightcurve orbit-{} {}>'.format(
            self.orbit.orbit_number,
            self.lightcurve_type.name)

    def __len__(self):
        """Return the length of the lightcurve (number of cadence points)"""
        return len(self.cadences)

    @hybrid_property
    def cadences(self):
        return self._cadences

    @cadences.setter
    def cadences(self, value):
        self._cadences = value

    @hybrid_property
    def bjd(self):
        return self._bjd

    @bjd.setter
    def bjd(self, value):
        self._bjd = value

    @hybrid_property
    def flux(self):
        return self._flux

    @flux.setter
    def flux(self, value):
        self._flux = value

    @hybrid_property
    def flux_err(self):
        return self._flux_err

    @flux_err.setter
    def flux_err(self, value):
        self._flux_err = value

    @hybrid_property
    def x_centroids(self):
        return self._x_centroids

    @x_centroids.setter
    def x_centroids(self, value):
        self._x_centroids = value

    @hybrid_property
    def y_centroids(self):
        return self._y_centroids

    @y_centroids.setter
    def y_centroids(self, value):
        self._y_centroids = value

    @hybrid_property
    def meta(self):
        return self._meta

    @meta.setter
    def meta(self, value):
        self._meta = value