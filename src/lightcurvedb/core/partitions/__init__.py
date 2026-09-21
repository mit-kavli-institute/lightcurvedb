"""PostgreSQL partition management for the LIST-partitioned tables.

This package operates on relation names and :class:`sqlalchemy.Connection`
objects only. It must not import :mod:`lightcurvedb.models` or
:mod:`lightcurvedb.io`, so that it stays usable against any partitioned
table in the metadata.

.. note::
   Not to be confused with :func:`lightcurvedb.util.iter.eq_partitions`,
   which splits an in-memory iterable into equal chunks and has nothing to
   do with PostgreSQL.
"""

from lightcurvedb.core.partitions.errors import (
    PartitionError,
    PartitionNameError,
    PartitionNameTooLongError,
)
from lightcurvedb.core.partitions.naming import (
    MAX_IDENTIFIER_BYTES,
    OBJECT_SUFFIXES,
    PartitionName,
)

__all__ = [
    "MAX_IDENTIFIER_BYTES",
    "OBJECT_SUFFIXES",
    "PartitionError",
    "PartitionName",
    "PartitionNameError",
    "PartitionNameTooLongError",
]
