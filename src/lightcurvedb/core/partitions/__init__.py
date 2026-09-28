"""PostgreSQL partition management for the LIST-partitioned tables.

This package operates on relation names and :class:`sqlalchemy.Connection`
objects only. It must not import :mod:`lightcurvedb.models` or
:mod:`lightcurvedb.io`, so that it stays usable against any partitioned
table in the metadata.

The modules form a stack. :mod:`~lightcurvedb.core.partitions.naming`
and :mod:`~lightcurvedb.core.partitions.errors` are pure;
:mod:`~lightcurvedb.core.partitions.catalog` only reads; and
:mod:`~lightcurvedb.core.partitions.ddl` and
:mod:`~lightcurvedb.core.partitions.bootstrap` emit DDL. Nothing in the
package commits -- the caller's transaction boundary is what makes a
sequence of statements atomic.

.. note::
   Not to be confused with :func:`lightcurvedb.util.iter.eq_partitions`,
   which splits an in-memory iterable into equal chunks and has nothing to
   do with PostgreSQL.
"""

from lightcurvedb.core.partitions.bootstrap import (
    ensure_default_partition,
    ensure_partition,
    ensure_staging_table,
    verify_relation_shape,
)
from lightcurvedb.core.partitions.catalog import (
    AttachabilityReport,
    ColumnSpec,
    ForeignKeySpec,
    IndexSpec,
    PartitionInfo,
    PartitionStrategy,
    check_attachable,
    column_mismatches,
    column_signature,
    default_partition_of,
    find_partition_for_value,
    foreign_key_definitions,
    index_definitions,
    list_table_partitions,
    parse_list_bound,
    partition_strategy,
    relation_kind,
    require_list_partitioned,
    resolve_qualified_name,
    resolve_table_name,
)
from lightcurvedb.core.partitions.ddl import (
    add_bound_check,
    analyze_relation,
    attach_partition,
    build_partition_indexes,
    create_staging_table,
    detach_partition,
    drop_relation,
    key_literal,
    lock_tables,
    mirror_outbound_foreign_keys,
    quoted_relation,
    referenced_tables,
    set_local_timeouts,
)
from lightcurvedb.core.partitions.errors import (
    AutocommitRequiredError,
    NotAttachableError,
    NotPartitionedError,
    PartitionError,
    PartitionNameError,
    PartitionNameTooLongError,
    RelationNotFoundError,
    StagingShapeMismatchError,
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
    "AutocommitRequiredError",
    "ColumnSpec",
    "ForeignKeySpec",
    "IndexSpec",
    "NotAttachableError",
    "NotPartitionedError",
    "PartitionError",
    "PartitionInfo",
    "PartitionName",
    "PartitionNameError",
    "PartitionNameTooLongError",
    "PartitionStrategy",
    "RelationNotFoundError",
    "StagingShapeMismatchError",
    "UnsupportedPartitionStrategyError",
    "add_bound_check",
    "analyze_relation",
    "attach_partition",
    "build_partition_indexes",
    "check_attachable",
    "column_mismatches",
    "column_signature",
    "create_staging_table",
    "default_partition_of",
    "detach_partition",
    "drop_relation",
    "ensure_default_partition",
    "ensure_partition",
    "ensure_staging_table",
    "find_partition_for_value",
    "foreign_key_definitions",
    "index_definitions",
    "key_literal",
    "list_table_partitions",
    "lock_tables",
    "mirror_outbound_foreign_keys",
    "parse_list_bound",
    "partition_strategy",
    "quoted_relation",
    "referenced_tables",
    "relation_kind",
    "require_list_partitioned",
    "resolve_qualified_name",
    "resolve_table_name",
    "set_local_timeouts",
    "verify_relation_shape",
]
