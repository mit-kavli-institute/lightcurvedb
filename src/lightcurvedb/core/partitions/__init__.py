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

from lightcurvedb.core.partitions.catalog import (
    AttachabilityReport,
    PartitionInfo,
    PartitionStrategy,
    check_attachable,
    default_partition_of,
    find_partition_for_value,
    list_table_partitions,
    parse_list_bound,
    partition_strategy,
    relation_kind,
    require_list_partitioned,
    resolve_table_name,
)
from lightcurvedb.core.partitions.errors import (
    NotAttachableError,
    NotPartitionedError,
    PartitionError,
    PartitionNameError,
    PartitionNameTooLongError,
    RelationNotFoundError,
    UnsupportedPartitionStrategyError,
)
from lightcurvedb.core.partitions.naming import (
    MAX_IDENTIFIER_BYTES,
    OBJECT_SUFFIXES,
    PartitionName,
)

__all__ = [
    "MAX_IDENTIFIER_BYTES",
    "OBJECT_SUFFIXES",
    "AttachabilityReport",
    "NotAttachableError",
    "NotPartitionedError",
    "PartitionError",
    "PartitionInfo",
    "PartitionName",
    "PartitionNameError",
    "PartitionNameTooLongError",
    "PartitionStrategy",
    "RelationNotFoundError",
    "UnsupportedPartitionStrategyError",
    "check_attachable",
    "default_partition_of",
    "find_partition_for_value",
    "list_table_partitions",
    "parse_list_bound",
    "partition_strategy",
    "relation_kind",
    "require_list_partitioned",
    "resolve_table_name",
]
