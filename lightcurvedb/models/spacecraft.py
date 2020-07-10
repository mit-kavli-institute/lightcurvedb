from sqlalchemy import Column, ForeignKey, Float, DateTime
from sqlalchemy.orm import relationship
from sqlalchemy.schema import UniqueConstraint
from sqlalchemy.ext.hybrid import hybrid_property
from lightcurvedb.core.base_model import QLPReference
from lightcurvedb.core.fields import high_precision_column

class SpacecraftEphemris(QLPReference):
    __tablename__ = 'spacecraftephemeris'

    barycentric_dynamical_time = Column(Float, primary_key=True)
    calendar_date = Column(DateTime, index=True)
    x_coordinate = high_precision_column()
    y_coordinate = high_precision_column()
    z_coordinate = high_precision_column()

    light_travel_time = high_precision_column()
    range_to = high_precision_column()
    range_rate = high_precision_column()

    def __repr__(self):
        return '<SpacecraftEph {} ({}, {}, {}) />'.format(
            self.barycentric_dynamical_time,
            self.x,
            self.y,
            self.x
        )

    @hybrid_property
    def x(self):
        return self.x_coordinate

    @x.expression
    def x(cls):
        return cls.x_coordinate

    @hybrid_property
    def y(self):
        return self.y_coordinate

    @y.expression
    def y(cls):
        return cls.y_coordinate

    @hybrid_property
    def z(self):
        return self.z_coordinate

    @z.expression
    def z(cls):
        return cls.z_coordinate
