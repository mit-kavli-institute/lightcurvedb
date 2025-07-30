import datetime
import pathlib
from decimal import Decimal
from typing import Any

import numpy as np
import sqlalchemy as sa
from numpy import typing as npt
from sqlalchemy import orm
from sqlalchemy.dialects.postgresql import JSONB

from lightcurvedb.core.types import NumpyArrayType


class LCDBModel(orm.DeclarativeBase):
    """
    A common SQLAlchemy base model for LCDB v2 models.
    """

    # Type Hint Registration
    type_annotation_map = {
        dict[str, Any]: JSONB,
        Decimal: sa.DOUBLE_PRECISION,
        npt.NDArray[np.int64]: NumpyArrayType(sa.BigInteger),
        npt.NDArray[np.int32]: NumpyArrayType(sa.Integer),
        npt.NDArray[np.int16]: NumpyArrayType(sa.SmallInteger),
        npt.NDArray[np.int8]: NumpyArrayType(
            sa.SmallInteger
        ),  # PostgreSQL has no TINYINT
        npt.NDArray[np.float64]: NumpyArrayType(sa.Float),
        npt.NDArray[np.float32]: NumpyArrayType(sa.REAL),
        npt.NDArray[np.bool_]: NumpyArrayType(sa.Boolean),
        pathlib.Path: sa.String,
    }


@orm.declarative_mixin
class CreatedOnMixin:
    """
    Mixin for describing QLP Dataproducts such as frames, lightcurves,
    and BLS results
    """

    created_on: orm.Mapped[datetime.datetime] = orm.mapped_column(
        server_default=sa.func.now()
    )


@orm.declarative_mixin
class NameAndDescriptionMixin:
    """
    Mixin for describing QLP data subtypes such as lightcurve types.
    """

    name: orm.Mapped[str] = orm.mapped_column(sa.String(64))
    description: orm.Mapped[str]
