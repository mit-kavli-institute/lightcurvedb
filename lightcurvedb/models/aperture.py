from sqlalchemy import (
    Column,
    String,
    Numeric,
    BigInteger,
    ForeignKey,
    SmallInteger,
)
from sqlalchemy.orm import relationship
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.schema import UniqueConstraint, CheckConstraint
from lightcurvedb.core.base_model import QLPReference


class Aperture(QLPReference):
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
    best_apertures : list of BestApertureMap
        Returns all mappings of BestApertureMap related to this Aperture.
        Accessing this attribute will result in a SQL query emission.
    """

    __tablename__ = "apertures"

    # Constraints
    __table_args__ = (
        UniqueConstraint("star_radius", "inner_radius", "outer_radius"),
        CheckConstraint("char_length(name) >= 1", name="minimum_name_length"),
    )

    # Model Attributes
    id = Column(SmallInteger)
    name = Column(String(64), primary_key=True)
    star_radius = Column(Numeric, nullable=False)
    inner_radius = Column(Numeric, nullable=False)
    outer_radius = Column(Numeric, nullable=False)

    # Relationships
    lightcurves = relationship("Lightcurve", back_populates="aperture")
    best_apertures = relationship("BestApertureMap", back_populates="aperture")

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

    @hybrid_property
    def id(self):
        return self.name

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


class BestApertureMap(QLPReference):
    """
    A mapping of lightcurves to their 'best' aperture. This model
    is defined so TICs will contain 1 best aperture. This is enforced
    on a PSQL constraint so behavior alterations will require a
    database migration.

    Attributes
    ----------
    aperture_id : str
        Foreign key to the "best aperture". Do not edit unless you're
        confident in the change will be a valid Foreign Key.
    tic_id : int
        The TIC identifier. This serves as the primary key of the model
        so it must be unique.

    aperture : Aperture
        Returns the Aperture model related to this BestApertureMap
        instance. Accessing this attribute will result in a SQL query
        emission.
    """

    __tablename__ = "best_apertures"
    __table_args__ = (UniqueConstraint("tic_id", name="best_ap_unique_tic"),)

    aperture_id = Column(
        ForeignKey(Aperture.name, onupdate="CASCADE", ondelete="RESTRICT"),
        primary_key=True,
    )
    tic_id = Column(BigInteger, primary_key=True)

    aperture = relationship("Aperture", back_populates="best_apertures")

    @classmethod
    def set_best_aperture(cls, tic_id, aperture):
        q = insert(cls.__table__)
        if isinstance(aperture, Aperture):
            q = q.values(
                tic_id=tic_id, aperture_id=aperture.name
            ).on_conflict_do_update(
                constraint="best_ap_unique_tic",
                set_={"aperture_id": aperture.name},
            )
        else:
            q = q.values(
                tic_id=tic_id, aperture_id=aperture
            ).on_conflict_do_update(
                constraint="best_ap_unique_tic", set_={"aperture_id": aperture}
            )
        return q
