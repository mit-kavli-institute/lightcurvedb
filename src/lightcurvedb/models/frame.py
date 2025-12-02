import pathlib
import typing

import sqlalchemy as sa
from sqlalchemy import orm

from lightcurvedb.core.base_model import CreatedOnMixin, LCDBModel

if typing.TYPE_CHECKING:
    from lightcurvedb.models import Observation


class FITSFrame(LCDBModel, CreatedOnMixin):
    """
    Represents a FITS (Flexible Image Transport System) frame.

    FITSFrame stores metadata about individual FITS files used in
    astronomical observations. It uses polymorphic inheritance to
    support different frame types while maintaining FITS standard
    compliance.

    Attributes
    ----------
    id : int
        Primary key identifier
    type : str
        Polymorphic discriminator for frame type
    cadence : int
        Time-ordered frame number
    observation_id : int
        Foreign key to parent observation
    simple : bool
        FITS primary keyword - file conforms to FITS standard
    bitpix : int
        FITS primary keyword - bits per pixel
    naxis : int
        FITS primary keyword - number of axes
    naxis_values : list[int]
        Array representation of NAXIS1, NAXIS2, etc.
    extended : bool
        FITS primary keyword - file may contain extensions
    file_path : Path, optional
        File system path to the FITS file

    Notes
    -----
    The type-cadence combination must be unique, enforced by
    database constraint. Polymorphic on 'type' field allows
    for specialized frame subclasses.
    """

    __tablename__ = "fits_frame"
    __mapper_args__ = {
        "polymorphic_identity": "basefits",
        "polymorphic_on": "type",
    }

    __table_args__ = (
        sa.UniqueConstraint(
            "type",
            "cadence",
            name="distinct_frame_type_cadence_idx",
        ),
    )

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    type: orm.Mapped[str] = orm.mapped_column(index=True)
    cadence: orm.Mapped[int] = orm.mapped_column(sa.BigInteger, index=True)

    observation_id: orm.Mapped[int] = orm.mapped_column(
        sa.ForeignKey("observation.id", ondelete="CASCADE"), index=True
    )

    # Define primary keywords
    simple: orm.Mapped[bool]
    bitpix: orm.Mapped[int]
    naxis: orm.Mapped[int]

    naxis_values: orm.Mapped[list[int]] = orm.mapped_column(
        sa.ARRAY(sa.Integer),
        comment="Representation of the required NAXIS[n] keywords",
    )
    extended: orm.Mapped[bool]
    file_path: orm.Mapped[typing.Optional[pathlib.Path]]

    observation: orm.Mapped["Observation"] = orm.relationship(
        "Observation", back_populates="fits_images"
    )
