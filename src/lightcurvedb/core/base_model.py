import datetime
import pathlib
from decimal import Decimal
from typing import Any

import numpy as np
import sqlalchemy as sa
from numpy import typing as npt
from sqlalchemy import orm
from sqlalchemy.dialects.postgresql import JSONB


class LCDBModel(orm.DeclarativeBase):
    """
    A common SQLAlchemy base model for LCDB v2 models.
    """

    # Type Hint Registration
    type_annotation_map = {
        dict[str, Any]: JSONB,
        Decimal: sa.DOUBLE_PRECISION,
        npt.NDArray[np.int64]: sa.ARRAY(sa.BigInteger),
        npt.NDArray[np.float64]: sa.ARRAY(sa.Float),
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
