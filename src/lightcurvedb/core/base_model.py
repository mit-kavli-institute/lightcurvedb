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


def _format_array_summary(arr) -> str:
    """Format a numpy array as a compact summary string."""
    if arr is None:
        return "None"
    if not isinstance(arr, np.ndarray):
        return repr(arr)
    if len(arr) <= 6:
        return repr(arr.tolist())
    dtype = arr.dtype
    return f"{dtype}[{len(arr)}] range=[{arr.min()}, {arr.max()}]"


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

    def __repr__(self) -> str:
        """Default repr for LCDB models. Shows class name and primary keys."""
        mapper = sa.inspect(self.__class__)
        pk_cols = [col.name for col in mapper.primary_key]
        pk_values = ", ".join(
            f"{col}={getattr(self, col, '?')!r}" for col in pk_cols
        )
        return f"<{self.__class__.__name__}({pk_values})>"

    def __rich_repr__(self):
        mapper = sa.inspect(self.__class__)
        pk_cols = [col.name for col in mapper.primary_key]
        for col in pk_cols:
            yield col, getattr(self, col, None)

    def __rich_console__(self, console, options):
        from rich.table import Table

        table = Table(title=self.__class__.__name__, show_header=True)
        table.add_column("Field", style="bold cyan")
        table.add_column("Value")
        mapper = sa.inspect(self.__class__)
        for col in mapper.columns:
            val = getattr(self, col.key, None)
            if isinstance(val, np.ndarray):
                display = _format_array_summary(val)
            else:
                display = repr(val)
            table.add_row(col.key, display)
        yield table


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
