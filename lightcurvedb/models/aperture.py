from sqlalchemy import Column, Integer, String, Float, Numeric
from sqlalchemy.orm import relationship
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.schema import UniqueConstraint, CheckConstraint
from lightcurvedb.core.base_model import QLPReference


class Aperture(QLPReference):
    """
        Provides ORM implementation of an aperture used by QLP
    """

    __tablename__ = 'apertures'

    # Constraints
    __table_args__ = (
        UniqueConstraint('star_radius', 'inner_radius', 'outer_radius'),
        CheckConstraint('char_length(name) >= 1', name='minimum_name_length')
    )

    # Model Attributes
    name = Column(String(64), unique=True, nullable=False)
    star_radius = Column(Numeric, nullable=False)
    inner_radius = Column(Numeric, nullable=False)
    outer_radius = Column(Numeric, nullable=False)

    # Relationships
    lightcurves = relationship('OrbitLightcurve', back_populates='aperture')

    def __str__(self):
        return '{}:{}:{}'.format(
            self.star_radius, self.inner_radius, self.outer_radius
        )

    def __repr__(self):
        return '<Aperture {} {} >'.format(self.name, str(self))

    @hybrid_property
    def star_r(self):
        return self.star_radius

    @hybrid_property
    def inner_r(self):
        return self.inner_radius

    @hybrid_property
    def outer_r(self):
        return self.outer_radius

    @classmethod
    def from_aperture_string(cls, string):
        """Attempt to parse an aperture string (fistar/fiphot format)
        Arguments
        ---------
        string: str
            An aperture string formatted such as 1.2:1.4:4.5
            This corresponds to the format of
            [star radius]:[inner radius]:[outer radius]

        """
        vals = tuple(string.split(':'))
        if len(vals) != 3:
            raise ValueError(
                'Given aperture string "{}" is not formatted correctly'.format(
                    string
                )
            )
        star_r = float(vals[0])
        inner_r = float(vals[1])
        outer_r = float(vals[2])

        return star_r, inner_r, outer_r
