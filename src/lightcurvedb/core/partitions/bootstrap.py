"""Idempotent provisioning of partitions and staging tables.

There is no migration tool in this project -- the schema comes from
``metadata.create_all`` -- so provisioning has to be safe to repeat.
Every function here can be called on each process start: it either finds
what it needs already in place and returns, or creates it.

"Already in place" is checked against ``pg_catalog``, never against a
registry, and anything ambiguous is refused rather than adopted. A
relation with the right name that is *not* what was asked for -- a
retired table where a staging table should go, or a different revision
already holding the slot -- raises instead of being silently accepted.

Transaction contract
--------------------
Every function takes a :class:`sqlalchemy.Connection`, emits DDL, and
never commits. The caller owns the transaction boundary.
"""

from __future__ import annotations

import sqlalchemy as sa

from lightcurvedb.core.partitions.catalog import (
    TableRef,
    column_mismatches,
    column_signature,
    default_partition_of,
    find_partition_for_value,
    relation_kind,
    require_list_partitioned,
    resolve_qualified_name,
)
from lightcurvedb.core.partitions.ddl import (
    create_staging_table,
    key_literal,
    quoted_relation,
)
from lightcurvedb.core.partitions.errors import (
    PartitionError,
    StagingShapeMismatchError,
)
from lightcurvedb.core.partitions.naming import PartitionName


def ensure_partition(
    conn: sa.Connection,
    table: TableRef,
    key_value: int,
    *,
    revision: int = 0,
    schema: str | None = None,
) -> PartitionName:
    """Make sure a live partition holds ``key_value``, creating one if not.

    This is the path for an observation that has no data yet. It uses
    ``CREATE TABLE ... PARTITION OF``, which creates the child indexes
    itself and needs no validation scan, and it is the *only*
    provisioning path that should be used on an empty slot. The
    create-then-attach route exists for replacing a populated partition,
    where the point is that the new data is loaded before anything is
    locked.

    Parameters
    ----------
    conn : sqlalchemy.Connection
        Connection to emit on. Nothing is committed.
    table : str or sqlalchemy.Table or object with ``__table__``
        The partitioned parent.
    key_value : int
        The LIST value to provision.
    revision : int, default 0
        Revision to name the partition with. The default produces the
        unversioned ``<base>_obs_<id>`` form a DBA would write by hand.
    schema : str, optional
        Schema to work in. Defaults to the parent's, else ``public``.

    Returns
    -------
    PartitionName
        The name of the partition now holding ``key_value``, whether
        this call created it or found it.

    Raises
    ------
    PartitionError
        If the slot is already held by a relation with a different name
        -- another revision is live, and replacing it is a swap, not a
        provisioning step -- or if a detached relation already occupies
        the name.
    """
    schema, parent = resolve_qualified_name(table, schema=schema)
    require_list_partitioned(conn, parent, schema=schema)
    name = PartitionName(parent, key_value, revision)

    live = find_partition_for_value(conn, parent, key_value, schema=schema)
    if live is not None:
        if live.name != name.table:
            raise PartitionError(
                f"{parent} already has {live.name} attached for "
                f"{key_value}. Promoting {name.table} in its place is a "
                "swap, not a provisioning step"
            )
        return name

    if relation_kind(conn, name.table, schema=schema) is not None:
        raise PartitionError(
            f"{schema}.{name.table} exists but is not attached to "
            f"{parent}. It is a staged or retired relation, and "
            "attaching it is a swap, not a provisioning step"
        )

    child = quoted_relation(conn, name.table, schema=schema)
    conn.execute(
        sa.text(
            f"CREATE TABLE {child} PARTITION OF "
            f"{quoted_relation(conn, parent, schema=schema)} "
            f"FOR VALUES IN ({key_literal(key_value)})"
        )
    )
    return name


def ensure_staging_table(
    conn: sa.Connection,
    table: TableRef,
    key_value: int,
    revision: int,
    *,
    schema: str | None = None,
) -> PartitionName:
    """Make sure an empty, detached staging table exists for a revision.

    Creates the table if it is absent and, either way, proves its
    columns still match the parent's before returning.

    Parameters
    ----------
    conn : sqlalchemy.Connection
        Connection to emit on. Nothing is committed.
    table : str or sqlalchemy.Table or object with ``__table__``
        The partitioned parent to take the shape from.
    key_value : int
        The LIST value this revision will hold.
    revision : int
        Which revision this is. Get it from
        :func:`~lightcurvedb.core.partitions.state.next_revision` rather
        than guessing, so repeat campaigns never collide.
    schema : str, optional
        Schema to work in. Defaults to the parent's, else ``public``.

    Returns
    -------
    PartitionName
        The staging table's name.

    Raises
    ------
    PartitionError
        If a relation of that name exists and is an attached partition
        -- that is live data, not a staging table.
    StagingShapeMismatchError
        If an existing table's columns no longer match the parent's.

    Notes
    -----
    The shape check is the point of this function. ``CREATE TABLE IF NOT
    EXISTS`` accepts a table built from an older definition of the
    parent without complaint, and the mismatch would then surface at
    attach time, under ``ACCESS EXCLUSIVE``, with the campaign already
    committed to the swap.
    """
    schema, parent = resolve_qualified_name(table, schema=schema)
    require_list_partitioned(conn, parent, schema=schema)
    name = PartitionName(parent, key_value, revision)

    if relation_kind(conn, name.table, schema=schema) is not None:
        live = find_partition_for_value(conn, parent, key_value, schema=schema)
        if live is not None and live.name == name.table:
            raise PartitionError(
                f"{schema}.{name.table} is attached to {parent} and holds "
                f"live data for {key_value}"
            )
    else:
        create_staging_table(
            conn, parent, name.table, key_value, schema=schema
        )

    verify_relation_shape(conn, parent, name.table, schema=schema)
    return name


def verify_relation_shape(
    conn: sa.Connection,
    table: TableRef,
    candidate: str,
    *,
    schema: str | None = None,
) -> None:
    """Prove ``candidate`` still has the parent's columns.

    Compares names, types, ``NOT NULL`` and physical position. Position
    is included here though ``ATTACH`` does not care about it: a
    difference in column order means the table was built from a
    different definition of the parent, which is exactly what this is
    looking for.

    Parameters
    ----------
    conn : sqlalchemy.Connection
        Connection to read with. Nothing is committed.
    table : str or sqlalchemy.Table or object with ``__table__``
        The partitioned parent.
    candidate : str
        The relation to check against it.
    schema : str, optional
        Schema for both. Defaults to the parent's, else ``public``.

    Raises
    ------
    StagingShapeMismatchError
        Listing every difference found.
    RelationNotFoundError
        If either relation is missing.
    """
    schema, parent = resolve_qualified_name(table, schema=schema)
    findings = column_mismatches(
        column_signature(conn, parent, schema=schema),
        column_signature(conn, candidate, schema=schema),
        compare_ordinals=True,
    )
    if findings:
        raise StagingShapeMismatchError(
            f"{schema}.{candidate} does not match {schema}.{parent}: "
            + "; ".join(findings)
        )


def ensure_default_partition(
    conn: sa.Connection, table: TableRef, *, schema: str | None = None
) -> str:
    """Make sure ``table`` has a DEFAULT partition. Development only.

    Parameters
    ----------
    conn : sqlalchemy.Connection
        Connection to emit on. Nothing is committed.
    table : str or sqlalchemy.Table or object with ``__table__``
        The partitioned parent.
    schema : str, optional
        Schema to work in. Defaults to the parent's, else ``public``.

    Returns
    -------
    str
        The DEFAULT partition's name, whether found or created.

    Warnings
    --------
    A DEFAULT partition is convenient in a test database and harmful in
    a production one. While it exists, every ``ATTACH`` must scan it to
    prove none of its rows belong in the incoming partition -- under
    ``ACCESS EXCLUSIVE``, and it fails outright if any row does -- and
    ``DETACH PARTITION ... CONCURRENTLY`` is refused entirely. Rows that
    land in it are also invisible to the replacement machinery, which
    works one bound at a time.
    """
    schema, parent = resolve_qualified_name(table, schema=schema)
    require_list_partitioned(conn, parent, schema=schema)
    existing = default_partition_of(conn, parent, schema=schema)
    if existing is not None:
        return existing.name

    name = f"{parent}_default"
    child = quoted_relation(conn, name, schema=schema)
    conn.execute(
        sa.text(
            f"CREATE TABLE {child} PARTITION OF "
            f"{quoted_relation(conn, parent, schema=schema)} DEFAULT"
        )
    )
    return name
