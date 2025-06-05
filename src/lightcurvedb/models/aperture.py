from typing import Union

from sqlalchemy import Numeric, SmallInteger
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.schema import CheckConstraint, UniqueConstraint

from lightcurvedb.core.base_model import (
    CreatedOnMixin,
    NameAndDescriptionMixin,
    QLPModel,
)


class Aperture(QLPModel, CreatedOnMixin, NameAndDescriptionMixin):
    """
    Provides ORM implementation of an aperture used by QLP.

    Attributes
    ----------
    name : str
        The name of the Aperture. This serves as the primary key of the Model
        so this is both unique and indexed. This name is case-sensitive.
    star_radius : float
        The star radius to be used in the fiphot/fistar processing.
    inner_radius : float
        The inner radius to be used in the fiphot/fistar processing.
    outer_radius : float
        The outer radius to be used in the fiphot/fistar processing.

    lightcurves : list of Lightcurves
        Returns all lightcurves associated with this Aperture. Accessing
        this attribute will result in a SQL query emission.
    """

    __tablename__ = "apertures"

    # Constraints
    __table_args__ = (
        UniqueConstraint("star_radius", "inner_radius", "outer_radius"),
        CheckConstraint("char_length(name) >= 1", name="minimum_name_length"),
    )

    # Model Attributes
    id: Mapped[int] = mapped_column(
        SmallInteger, primary_key=True, unique=True
    )
    star_radius: Mapped[Union[float, None]] = mapped_column(
        Numeric, nullable=True
    )
    inner_radius: Mapped[Union[float, None]] = mapped_column(
        Numeric, nullable=True
    )
    outer_radius: Mapped[Union[float, None]] = mapped_column(
        Numeric, nullable=True
    )

    # Relationships
    lightcurves = relationship(
        "ArrayOrbitLightcurve", back_populates="aperture"
    )

    def __str__(self):
        return self.name

    def __repr__(self):
        return "<Aperture {0} {1} >".format(self.name, self.format())

    def format(self):
        return "{0}:{1}:{2}".format(
            self.star_radius, self.inner_radius, self.outer_radius
        )

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
            `[star radius]:[inner radius]:[outer radius]`.
            If empty, returns `(None, None, None)`.
        """
        if len(string) == 0:
            return None, None, None
        vals = tuple(string.split(":"))
        if len(vals) != 3:
            raise ValueError(
                "Given aperture string "
                '"{0}" is not formatted correctly'.format(string)
            )
        star_r = float(vals[0])
        inner_r = float(vals[1])
        outer_r = float(vals[2])

        return star_r, inner_r, outer_r
