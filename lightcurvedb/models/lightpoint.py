from lightcurvedb.core.base_model import QLPModel, DynamicIdMixin
from sqlalchemy import BigInteger, Column, ForeignKey, Integer
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION, insert
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import backref, relationship
from sqlalchemy.schema import UniqueConstraint


class Lightpoint(QLPModel, DynamicIdMixin('lightpoints')):
    """Encompasses a single instance of a lightcurve exposure."""

    __tablename__ = 'lightpoints'

    # Constraints
    __table_args__ = (
        UniqueConstraint('lightcurve_id', 'cadence', name='lc_cadence_unique'),
    )

    cadence = Column(Integer, nullable=False, index=True)
    barycentric_julian_date = Column(DOUBLE_PRECISION, nullable=False, index=True)

    # To maintain flux<->magnitude agnosticism just consider this
    # column to have a 'value'. Let the lightcurve model determine
    # what the datatype is
    value = Column(DOUBLE_PRECISION, nullable=False)
    error = Column(DOUBLE_PRECISION)

    x_centroid = Column(DOUBLE_PRECISION, nullable=False)
    y_centroid = Column(DOUBLE_PRECISION, nullable=False)

    quality_flag = Column(Integer, nullable=False)

    # Relationships
    lightcurve_id = Column(ForeignKey('lightcurves.id', ondelete='CASCADE', onupdate='CASCADE'), index=True, nullable=False)
    lightcurve = relationship('Lightcurve', back_populates='lightpoints')

    @hybrid_property
    def bjd(self):
        return self.barycentric_julian_date

    @hybrid_property
    def x(self):
        return self.x_centroid

    @hybrid_property
    def y(self):
        return self.y_centroid

    @classmethod
    def bulk_upsert_stmt(cls, values):
        cols = [
            'barycentric_julian_date',
            'value',
            'error',
            'x_centroid',
            'y_centroid',
            'quality_flag'
        ]
        stmt = insert(cls).values(values)
        stmt = stmt.on_conflict_do_update(
            constraint='lc_cadence_unique',
            set_={
                c: getattr(stmt.excluded, c) for c in cols
            }
        )
        return stmt


LOOKUP = {
    'cadence': Lightpoint.cadence,
    'bjd': Lightpoint.barycentric_julian_date,
    'value': Lightpoint.value,
    'error': Lightpoint.error,
    'x_centroid': Lightpoint.x_centroid,
    'y_centroid': Lightpoint.y_centroid,
    'quality_flag': Lightpoint.quality_flag
}
