from datetime import datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import Mapped, mapped_column

from lightcurvedb.core.base_model import CreatedOnMixin, QLPModel


class SpacecraftEphemeris(QLPModel, CreatedOnMixin):
    __tablename__ = "spacecraftephemeris"

    id: Mapped[int] = mapped_column(primary_key=True)
    barycentric_dynamical_time: Mapped[Decimal] = mapped_column(unique=True)
    calendar_date: Mapped[Optional[datetime]] = mapped_column(index=True)
    x_coordinate: Mapped[Decimal]
    y_coordinate: Mapped[Decimal]
    z_coordinate: Mapped[Decimal]

    light_travel_time: Mapped[Decimal]
    range_to: Mapped[Decimal]
    range_rate: Mapped[Decimal]

    def __repr__(self):
        return (
            "<SpacecraftEph {barycentric_dynamical_time} "
            "({x}, {y}, {z}) />".format(**self.to_dict)
        )

    @property
    def to_dict(self):
        return {
            "barycentric_dynamical_time": self.barycentric_dynamical_time,
            "x": self.x,
            "y": self.y,
            "z": self.x,
        }

    @hybrid_property
    def bjd(self):
        return self.barycentric_dynamical_time

    @bjd.expression
    def bjd(cls):
        return cls.barycentric_dynamical_time

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
