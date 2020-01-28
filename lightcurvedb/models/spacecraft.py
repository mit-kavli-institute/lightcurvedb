from sqlalchemy import Column, ForeignKey, Float, DateTime
from sqlalchemy.orm import relationship
from lightcurvedb.core.base_model import QLPReference
from lightcurvedb.core.fields import high_precision_column

class SpacecraftEphemeris(QLPReference):
    __tablename__ = 'spacecraftephemeris'

    jdtdb = Column(Float, index=True)
    tdb = Column(DateTime, index=True)
    x_coordinate = high_precision_column()
    y_coordinate = high_precision_column()
    z_coordinate = high_precision_column()

    light_travel_time = high_precision_column()
    range_to = high_precision_column()
    range_rate = high_precision_column()
