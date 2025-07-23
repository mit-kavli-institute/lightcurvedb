"""
Custom SQLAlchemy types for the lightcurve database.

This module provides TypeDecorator implementations for handling
special data types, particularly for converting between database
representations and Python objects.
"""

from typing import Any, Optional, Type

import numpy as np
from sqlalchemy import (
    ARRAY,
    REAL,
    BigInteger,
    Boolean,
    Float,
    Integer,
    SmallInteger,
    TypeDecorator,
)
from sqlalchemy.engine import Dialect


class NumpyArrayType(TypeDecorator):
    """
    TypeDecorator that converts PostgreSQL arrays to numpy arrays.

    This type automatically handles conversion between PostgreSQL ARRAY
    columns and numpy arrays, providing seamless integration for scientific
    computing workflows.

    Parameters
    ----------
    item_type : TypeEngine
        The SQLAlchemy type of array elements (e.g., BigInteger, Integer,
        Float)

    Examples
    --------
    >>> from sqlalchemy import Column, Integer
    >>> from lightcurvedb.core.types import NumpyArrayType
    >>>
    >>> class MyModel(Base):
    ...     values = Column(NumpyArrayType(Integer))
    ...     # This will store as PostgreSQL integer[] but return numpy arrays

    >>> # Using with SQLAlchemy 2.0 mapped columns
    >>> from sqlalchemy import orm
    >>> from numpy import typing as npt
    >>>
    >>> class MyModel(Base):
    ...     int_array: orm.Mapped[npt.NDArray[np.int32]]
    ...     float_array: orm.Mapped[npt.NDArray[np.float64]]

    Notes
    -----
    The numpy dtype is automatically determined based on the SQL type:

    Supported mappings:
    - SmallInteger -> np.int16 (16-bit signed integer)
    - Integer -> np.int32 (32-bit signed integer)
    - BigInteger -> np.int64 (64-bit signed integer)
    - REAL -> np.float32 (32-bit float, single precision)
    - Float -> np.float64 (64-bit float, double precision)
    - Boolean -> np.bool_ (boolean type)

    Special cases:
    - np.int8 maps to SmallInteger (PostgreSQL has no TINYINT)
    - Unsigned integers are not directly supported by PostgreSQL
    - Complex numbers have no PostgreSQL equivalent
    - 128-bit types exceed PostgreSQL numeric limits

    When arrays are retrieved from the database, they are automatically
    converted to numpy arrays with the appropriate dtype. When storing,
    numpy arrays are converted to Python lists for database compatibility.
    """

    impl = ARRAY
    cache_ok = True

    def __init__(self, item_type: Type, *args, **kwargs):
        """
        Initialize the NumpyArrayType with a specific item type.

        Parameters
        ----------
        item_type : Type
            The SQLAlchemy type for array elements
        *args, **kwargs
            Additional arguments passed to ARRAY constructor
        """
        self.item_type = item_type
        super().__init__(item_type, *args, **kwargs)

    def process_result_value(
        self, value: Optional[list], dialect: Dialect
    ) -> Optional[np.ndarray]:
        """
        Convert list from database to numpy array.

        Parameters
        ----------
        value : list or None
            The list value from the database
        dialect : Dialect
            The database dialect

        Returns
        -------
        numpy.ndarray or None
            The converted numpy array or None if value is None
        """
        if value is not None:
            dtype = self._get_numpy_dtype()
            return np.array(value, dtype=dtype)
        return value

    def process_bind_param(
        self, value: Any, dialect: Dialect
    ) -> Optional[list]:
        """
        Convert numpy array to list for database storage.

        Parameters
        ----------
        value : numpy.ndarray or list or None
            The value to store in the database
        dialect : Dialect
            The database dialect

        Returns
        -------
        list or None
            The value converted to a list for storage
        """
        if value is not None and isinstance(value, np.ndarray):
            return value.tolist()
        return value

    def coerce_compared_value(self, op: Any, value: Any) -> Any:
        """
        Maintain proper type coercion for comparison operators.

        This ensures that operations like indexing and comparisons
        work correctly with the underlying ARRAY type.

        Parameters
        ----------
        op : Any
            The comparison operator
        value : Any
            The value being compared

        Returns
        -------
        Any
            The coerced value
        """
        return self.impl.coerce_compared_value(op, value)

    def _get_numpy_dtype(self) -> Optional[Type]:
        """
        Map SQL types to numpy dtypes.

        Returns
        -------
        numpy.dtype or None
            The appropriate numpy dtype for the SQL type

        Notes
        -----
        PostgreSQL type mappings:
        - SmallInteger (SMALLINT): -32768 to 32767 → np.int16
        - Integer (INTEGER): -2147483648 to 2147483647 → np.int32
        - BigInteger (BIGINT): -9223372036854775808 to 9223372036854775807
          → np.int64
        - REAL: 6 decimal digits precision → np.float32
        - Float (DOUBLE PRECISION): 15 decimal digits precision → np.float64
        - Boolean: true/false → np.bool_

        Types not supported:
        - int8: PostgreSQL has no TINYINT, using SmallInteger
        - Unsigned integers: PostgreSQL has no unsigned types
        - Complex numbers: No PostgreSQL equivalent
        - 128-bit types: Exceed PostgreSQL numeric limits
        """
        # Create instances for comparison
        # Order matters: check more specific types first
        type_map = [
            (SmallInteger, np.int16),  # 16-bit signed integer
            (BigInteger, np.int64),  # 64-bit signed integer
            (
                Integer,
                np.int32,
            ),  # 32-bit signed integer (check after Small/Big)
            (REAL, np.float32),  # 32-bit float (single precision)
            (Float, np.float64),  # 64-bit float (double precision)
            (Boolean, np.bool_),  # Boolean type
        ]

        # Check both type and instance
        for sql_type, numpy_dtype in type_map:
            if isinstance(self.item_type, sql_type):
                return numpy_dtype
            if isinstance(self.item_type, type) and issubclass(
                self.item_type, sql_type
            ):
                return numpy_dtype

        # Default to None to let numpy infer the dtype
        return None
