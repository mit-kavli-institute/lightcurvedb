"""Emitting partition DDL.

Building a standalone table shaped like a partition, giving it the
parent's indexes and foreign keys, and attaching, detaching or dropping
it. Each function emits one statement, or one tightly-coupled group, so
that a caller composes the sequence it needs and owns the transaction
around it.

The order is load-bearing: **create, load, index, add foreign keys,
attach**. Indexing before the load pays a per-row maintenance cost for
nothing, and attaching before indexing makes PostgreSQL build the index
while holding ``ACCESS EXCLUSIVE`` on the parent -- which blocks every
reader of every partition, not just this one.

Transaction contract
--------------------
Every function takes a :class:`sqlalchemy.Connection`, emits DDL, and
never commits. PostgreSQL's DDL is transactional, so the caller's
transaction boundary is what makes a sequence of these atomic; that is
the whole basis of :mod:`lightcurvedb.core.partitions.swap`.

The exception is :func:`detach_partition` with ``concurrently=True``.
PostgreSQL refuses to run that inside a transaction block, so it needs a
connection opened with
``execution_options(isolation_level="AUTOCOMMIT")`` and raises
:class:`~lightcurvedb.core.partitions.AutocommitRequiredError` before
issuing anything otherwise.

.. note::
   :func:`lightcurvedb.util.iter.eq_partitions` splits an in-memory
   iterable into equal chunks. It has nothing to do with PostgreSQL and
   nothing to do with this module.
"""

from __future__ import annotations

import operator
import re
from collections.abc import Iterable
from typing import Final, Literal

import sqlalchemy as sa

from lightcurvedb.core.partitions.catalog import (
    TableRef,
    check_attachable,
    foreign_key_definitions,
    index_definitions,
    relation_kind,
    require_list_partitioned,
    resolve_qualified_name,
)
from lightcurvedb.core.partitions.errors import (
    AutocommitRequiredError,
    PartitionError,
    PartitionNameTooLongError,
)
from lightcurvedb.core.partitions.naming import MAX_IDENTIFIER_BYTES

#: Lock modes :func:`lock_tables` accepts, weakest to strongest.
LockMode = Literal[
    "ACCESS SHARE",
    "ROW SHARE",
    "ROW EXCLUSIVE",
    "SHARE UPDATE EXCLUSIVE",
    "SHARE",
    "SHARE ROW EXCLUSIVE",
    "EXCLUSIVE",
    "ACCESS EXCLUSIVE",
]

_LOCK_MODES: Final[frozenset[str]] = frozenset(
    {
        "ACCESS SHARE",
        "ROW SHARE",
        "ROW EXCLUSIVE",
        "SHARE UPDATE EXCLUSIVE",
        "SHARE",
        "SHARE ROW EXCLUSIVE",
        "EXCLUSIVE",
        "ACCESS EXCLUSIVE",
    }
)

# Values for SET LOCAL. Neither can be a bind parameter, so both are
# pattern-checked before they reach a statement.
_DURATION: Final = re.compile(r"^(?:0|[1-9][0-9]*)(?:us|ms|s|min|h|d)?$")
_MEMORY: Final = re.compile(r"^(?:[1-9][0-9]*)(?:kB|MB|GB|TB)?$")

_INDEX_PREFIXES: Final[tuple[str, ...]] = ("ix_", "idx_", "ux_", "uq_")


# ---------------------------------------------------------------------------
# Identifiers and literals
# ---------------------------------------------------------------------------


def _qualified(conn: sa.Connection, schema: str, name: str) -> str:
    quote = conn.dialect.identifier_preparer.quote
    return f"{quote(schema)}.{quote(name)}"


def quoted_relation(
    conn: sa.Connection, table: TableRef, *, schema: str | None = None
) -> str:
    """Render a schema-qualified, quoted relation name.

    Identifiers cannot travel as bind parameters, so anything
    interpolated into a DDL string goes through the dialect's identifier
    preparer first.

    Parameters
    ----------
    conn : sqlalchemy.Connection
        Connection whose dialect supplies the quoting rules.
    table : str or sqlalchemy.Table or object with ``__table__``
        The relation to name.
    schema : str, optional
        Schema override. Defaults to the table's own, else ``public``.

    Returns
    -------
    str
        Something like ``"public"."dataset_obs_5_v3"``.
    """
    return _qualified(conn, *resolve_qualified_name(table, schema=schema))


def key_literal(value: int) -> str:
    """Render a LIST key as a bare integer literal.

    ``CHECK (observation_id = 5)`` compiles to ``int4eq(Var, Const)``,
    structurally identical to the qual ``get_qual_for_list`` builds for
    the partition, so the prover discharges it and ``ATTACH`` skips the
    validation scan. Writing ``5::bigint`` instead yields ``int48eq``,
    the proof fails, and every row is read under ``ACCESS EXCLUSIVE``.
    A bind parameter is not an option: PostgreSQL does not accept one in
    DDL.
    """
    return str(operator.index(value))


def _derived(relation: str, suffix: str) -> str:
    """Name an object owned by ``relation``.

    Identical to :meth:`PartitionName.derived` for a canonical partition
    name, and defined here as well so that a staging table named by hand
    gets the same 63-byte guarantee.
    """
    name = f"{relation}_{suffix}"
    if len(name.encode()) > MAX_IDENTIFIER_BYTES:
        raise PartitionNameTooLongError(
            f"{name!r} is {len(name.encode())} bytes, over PostgreSQL's "
            f"limit of {MAX_IDENTIFIER_BYTES}"
        )
    return name


def _index_suffix(index_name: str, parent: str) -> str:
    """Derive a child index suffix from the parent's index name.

    ``ix_dataset_target_id`` on ``dataset`` gives ``target_idx``, and
    ``ix_datasethierarchy_source`` gives ``source_idx``. Deriving beats
    a lookup table: an index a DBA adds out of band gets a sensible
    child name without this module being taught about it.
    """
    stem = index_name
    for prefix in _INDEX_PREFIXES:
        if stem.startswith(prefix):
            stem = stem.removeprefix(prefix)
            break
    stem = stem.removeprefix(f"{parent}_").removesuffix("_id")
    stem = re.sub(r"[^a-z0-9_]+", "_", stem.lower()).strip("_")
    return f"{stem}_idx" if stem else "idx"


def _unique_suffix(taken: set[str], suffix: str) -> str:
    """Disambiguate a derived suffix that two parent objects share."""
    if suffix not in taken:
        taken.add(suffix)
        return suffix
    ordinal = 2
    while f"{suffix}{ordinal}" in taken:
        ordinal += 1
    taken.add(f"{suffix}{ordinal}")
    return f"{suffix}{ordinal}"


def _constraint_exists(
    conn: sa.Connection, schema: str, relation: str, name: str
) -> bool:
    return bool(
        conn.execute(
            sa.text(
                "SELECT 1 FROM pg_constraint k "
                "JOIN pg_class c ON c.oid = k.conrelid "
                "JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = :schema AND c.relname = :relation "
                "  AND k.conname = :name"
            ),
            {"schema": schema, "relation": relation, "name": name},
        ).scalar()
    )


# ---------------------------------------------------------------------------
# Building a candidate
# ---------------------------------------------------------------------------


def create_staging_table(
    conn: sa.Connection,
    table: TableRef,
    candidate: str,
    key_value: int,
    *,
    schema: str | None = None,
    partition_schema: str | None = None,
    if_not_exists: bool = False,
) -> None:
    """Create a standalone table shaped like a partition of ``table``.

    The table is a plain heap, not attached to anything, so it can be
    loaded, indexed and inspected while the partition it will eventually
    replace stays live. Loading a staging table rather than routing rows
    through the partitioned parent is also 18-75x faster, since it skips
    tuple routing and per-row foreign-key triggers.

    Parameters
    ----------
    conn : sqlalchemy.Connection
        Connection to emit on. Nothing is committed.
    table : str or sqlalchemy.Table or object with ``__table__``
        The partitioned parent to copy the shape of.
    candidate : str
        Name for the new table, in the parent's schema.
    key_value : int
        The LIST value this table will hold, written into a bound
        constraint named ``<candidate>_partcheck``.
    schema : str, optional
        Schema of the partitioned parent. Defaults to the parent's own,
        else ``public``.
    partition_schema : str, optional
        Schema the child partitions live in. Defaults to the parent's,
        which is the layout PostgreSQL produces unless a schema is named.
    if_not_exists : bool, default False
        Suppress the error when the table already exists. See the
        warning below before turning this on.

    Raises
    ------
    NotPartitionedError, UnsupportedPartitionStrategyError
        If the parent is not LIST-partitioned on a single column.
    PartitionNameTooLongError
        If the constraint name would exceed 63 bytes.

    Notes
    -----
    ``LIKE ... INCLUDING ALL EXCLUDING INDEXES`` carries over column
    names, types and order; ``NOT NULL`` on every column; CHECK
    constraints; defaults; and -- load-bearing where every row TOASTs --
    per-column storage and compression settings. It carries over no
    foreign keys and not the ``PARTITION BY`` clause, which is what
    makes the copy a plain heap that can be attached later.

    The bound constraint is declared inline rather than added
    afterwards, so it is ``convalidated`` from birth and every row
    loaded is checked against it for free. A validated, structurally
    matching CHECK is what lets ``ATTACH`` skip its scan.

    .. warning::
       ``if_not_exists=True`` accepts a table left over from an older
       definition of the parent without complaint, and the mismatch then
       surfaces at attach time under ``ACCESS EXCLUSIVE``. Prefer
       :func:`~lightcurvedb.core.partitions.bootstrap.ensure_staging_table`,
       which follows up with a column comparison.
    """
    schema, parent = resolve_qualified_name(table, schema=schema)
    partition_schema = partition_schema or schema
    strategy = require_list_partitioned(conn, parent, schema=schema)
    quote = conn.dialect.identifier_preparer.quote
    check = _derived(candidate, "partcheck")
    guard = "IF NOT EXISTS " if if_not_exists else ""
    conn.execute(
        sa.text(
            f"CREATE TABLE {guard}"
            f"{_qualified(conn, partition_schema, candidate)} ("
            f"LIKE {_qualified(conn, schema, parent)} "
            f"INCLUDING ALL EXCLUDING INDEXES, "
            f"CONSTRAINT {quote(check)} CHECK ("
            f"{quote(strategy.key_column)} = {key_literal(key_value)}))"
        )
    )


def add_bound_check(
    conn: sa.Connection,
    relation: str,
    key_column: str,
    key_value: int,
    *,
    schema: str | None = None,
    name: str | None = None,
) -> str | None:
    """Add the bound-implying ``CHECK`` to a standalone relation.

    Needed on a table that was detached rather than built: a plain
    ``DETACH`` leaves no CHECK behind, so re-attaching the relation
    later would re-read every row. Adding the constraint immediately
    after detaching keeps a rollback as cheap as the promotion was.

    Parameters
    ----------
    conn : sqlalchemy.Connection
        Connection to emit on. Nothing is committed.
    relation : str
        The standalone table to constrain.
    key_column : str
        The parent's partition key column.
    key_value : int
        The value this relation holds.
    schema : str, optional
        Schema of the relation. Defaults to ``public``.
    name : str, optional
        Constraint name. Defaults to ``<relation>_partcheck``.

    Returns
    -------
    str or None
        The constraint name, or ``None`` if it was already present.

    Notes
    -----
    Adding a CHECK scans the table to validate it, but on a relation
    nothing else can see that scan costs no concurrency. The alternative
    -- ``NOT VALID`` -- is worse than useless here:
    ``ConstraintImpliedByRelConstraint`` skips unvalidated entries, so
    the prover would not see it and ``ATTACH`` would scan anyway.
    """
    schema = schema or "public"
    constraint = name or _derived(relation, "partcheck")
    if _constraint_exists(conn, schema, relation, constraint):
        return None
    quote = conn.dialect.identifier_preparer.quote
    conn.execute(
        sa.text(
            f"ALTER TABLE {_qualified(conn, schema, relation)} "
            f"ADD CONSTRAINT {quote(constraint)} CHECK ("
            f"{quote(key_column)} = {key_literal(key_value)})"
        )
    )
    return constraint


def build_partition_indexes(
    conn: sa.Connection,
    table: TableRef,
    candidate: str,
    *,
    schema: str | None = None,
    partition_schema: str | None = None,
    maintenance_work_mem: str | None = None,
    max_parallel_maintenance_workers: int | None = None,
) -> tuple[str, ...]:
    """Reproduce the parent's indexes on ``candidate``.

    Run this after the bulk load and before the attach. Index names are
    derived from the parent's -- ``ix_dataset_target_id`` becomes
    ``<candidate>_target_idx`` -- so an index added to the parent out of
    band is reproduced without this module knowing about it.

    Parameters
    ----------
    conn : sqlalchemy.Connection
        Connection to emit on. Nothing is committed.
    table : str or sqlalchemy.Table or object with ``__table__``
        The partitioned parent whose index set to copy.
    candidate : str
        The standalone table to build them on.
    schema : str, optional
        Schema of the partitioned parent. Defaults to the parent's own,
        else ``public``.
    partition_schema : str, optional
        Schema the child partitions live in. Defaults to the parent's,
        which is the layout PostgreSQL produces unless a schema is named.
    maintenance_work_mem : str, optional
        ``SET LOCAL maintenance_work_mem`` for the duration, e.g.
        ``"4GB"``. Index builds spill to disk without it.
    max_parallel_maintenance_workers : int, optional
        ``SET LOCAL max_parallel_maintenance_workers`` for the duration.

    Returns
    -------
    tuple of str
        Names of the objects created. Already-present objects are
        skipped and not listed, so a second call returns ``()``.

    Raises
    ------
    PartitionError
        If an index definition cannot be parsed and so cannot be
        re-emitted safely.
    PartitionNameTooLongError
        If a derived name would exceed 63 bytes.

    Notes
    -----
    A primary key is re-emitted as ``ADD CONSTRAINT ... PRIMARY KEY``,
    never as ``CREATE UNIQUE INDEX``. ``AttachPartitionEnsureIndexes``
    adopts a child index only when it is backed by a constraint of the
    same kind as the parent's; a matching-but-constraintless unique
    index is rejected and PostgreSQL rebuilds it during ``ATTACH``,
    under ``ACCESS EXCLUSIVE``. That single difference is what turns a
    millisecond swap into an outage.

    Objects that already exist are left alone and left out of the
    return value, so a resumed run reports only what it actually did.

    Builds are deliberately not ``CONCURRENTLY``: the table is invisible
    to everyone else, so a full lock costs nothing, and a non-concurrent
    build is transactional and leaves no ``indisvalid = false`` debris
    behind if it fails.
    """
    schema, parent = resolve_qualified_name(table, schema=schema)
    partition_schema = partition_schema or schema
    quote = conn.dialect.identifier_preparer.quote
    target = _qualified(conn, partition_schema, candidate)

    if maintenance_work_mem is not None:
        if not _MEMORY.match(maintenance_work_mem):
            raise ValueError(
                f"invalid maintenance_work_mem: {maintenance_work_mem!r}"
            )
        conn.execute(
            sa.text(
                f"SET LOCAL maintenance_work_mem = '{maintenance_work_mem}'"
            )
        )
    if max_parallel_maintenance_workers is not None:
        workers = operator.index(max_parallel_maintenance_workers)
        conn.execute(
            sa.text(f"SET LOCAL max_parallel_maintenance_workers = {workers}")
        )

    created: list[str] = []
    taken: set[str] = set()
    for spec in index_definitions(conn, parent, schema=schema):
        if spec.constraint_backed and spec.constraint_definition:
            stem = (
                "pkey"
                if spec.constraint_type == "p"
                else _index_suffix(spec.name, parent).removesuffix("_idx")
                + "_key"
            )
            name = _derived(candidate, _unique_suffix(taken, stem))
            if _constraint_exists(conn, partition_schema, candidate, name):
                continue
            conn.execute(
                sa.text(
                    f"ALTER TABLE {target} ADD CONSTRAINT {quote(name)} "
                    f"{spec.constraint_definition}"
                )
            )
        else:
            body = spec.body
            if body is None:
                raise PartitionError(
                    f"cannot re-emit index {spec.name!r}: unrecognised "
                    f"definition {spec.definition!r}"
                )
            unique = "UNIQUE " if spec.unique else ""
            suffix = _unique_suffix(taken, _index_suffix(spec.name, parent))
            name = _derived(candidate, suffix)
            if relation_kind(conn, name, schema=partition_schema) is not None:
                continue
            conn.execute(
                sa.text(
                    f"CREATE {unique}INDEX {quote(name)} ON {target} {body}"
                )
            )
        created.append(name)
    return tuple(created)


def mirror_outbound_foreign_keys(
    conn: sa.Connection,
    table: TableRef,
    candidate: str,
    *,
    schema: str | None = None,
    partition_schema: str | None = None,
    include_partitioned_referents: bool = False,
) -> tuple[str, ...]:
    """Copy the parent's outbound foreign keys onto ``candidate``.

    A validated, structurally matching foreign key on the candidate is
    *adopted* at attach time rather than cloned and re-verified, which
    is the difference between 3.5 ms and 209 ms on a 300k-row partition.
    Creating them here moves that verification outside the lock window.

    Parameters
    ----------
    conn : sqlalchemy.Connection
        Connection to emit on. Nothing is committed.
    table : str or sqlalchemy.Table or object with ``__table__``
        The partitioned parent whose keys to copy.
    candidate : str
        The standalone table to put them on.
    schema : str, optional
        Schema of the partitioned parent. Defaults to the parent's own,
        else ``public``.
    partition_schema : str, optional
        Schema the child partitions live in. Defaults to the parent's,
        which is the layout PostgreSQL produces unless a schema is named.
    include_partitioned_referents : bool, default False
        Also mirror keys pointing at partitioned tables. See the warning.

    Returns
    -------
    tuple of str
        Names of the constraints created; already-present ones are
        skipped, so a second call returns ``()``.

    Warnings
    --------
    Keys pointing at a **partitioned** table are skipped by default, and
    the default is the only safe setting during a replacement. Such a
    key on a detached staging table is still a real dependency on the
    referent's live partitions: PostgreSQL then refuses to detach the
    partition being replaced, reporting that its keys are "still
    referenced from" the staging table, and the swap cannot proceed.
    Since PostgreSQL 14 rejects ``NOT VALID`` foreign keys on
    partitioned tables, there is no way to pre-validate them either --
    that verification has to happen inside the swap window.
    """
    schema, parent = resolve_qualified_name(table, schema=schema)
    partition_schema = partition_schema or schema
    quote = conn.dialect.identifier_preparer.quote
    target = _qualified(conn, partition_schema, candidate)

    created: list[str] = []
    taken: set[str] = set()
    for index, spec in enumerate(
        foreign_key_definitions(conn, parent, schema=schema)
    ):
        if (
            spec.referenced_is_partitioned
            and not include_partitioned_referents
        ):
            continue
        stem = spec.name.removeprefix(f"{parent}_") or f"fk{index}"
        name = _derived(candidate, _unique_suffix(taken, stem))
        if _constraint_exists(conn, partition_schema, candidate, name):
            continue
        conn.execute(
            sa.text(
                f"ALTER TABLE {target} ADD CONSTRAINT {quote(name)} "
                f"{spec.definition}"
            )
        )
        created.append(name)
    return tuple(created)


def analyze_relation(
    conn: sa.Connection, relation: str, *, schema: str | None = None
) -> None:
    """``ANALYZE`` a relation.

    Not optional before a swap. A freshly attached partition with no
    ``pg_statistic`` rows draws default selectivity estimates, and plans
    that used to nested-loop can flip to sequential scans the moment it
    goes live. Note also that PostgreSQL 14 autovacuum never analyses a
    partitioned *parent*, so the parent needs a periodic manual
    ``ANALYZE`` of its own, outside any swap window.
    """
    schema = schema or "public"
    conn.execute(sa.text(f"ANALYZE {_qualified(conn, schema, relation)}"))


# ---------------------------------------------------------------------------
# Attaching, detaching, dropping
# ---------------------------------------------------------------------------


def attach_partition(
    conn: sa.Connection,
    table: TableRef,
    candidate: str,
    key_value: int,
    *,
    schema: str | None = None,
    partition_schema: str | None = None,
    preflight: bool = True,
    allow_expensive: bool = True,
) -> None:
    """Attach ``candidate`` as the partition holding ``key_value``.

    Takes ``ACCESS EXCLUSIVE`` on the parent for the duration, which
    blocks every reader of every partition -- so what PostgreSQL has to
    do while holding it is the only thing that matters. With a validated
    bound CHECK, adopted indexes and adopted foreign keys, this is a
    catalog-only change measured in single-digit milliseconds.

    Parameters
    ----------
    conn : sqlalchemy.Connection
        Connection to emit on. Nothing is committed.
    table : str or sqlalchemy.Table or object with ``__table__``
        The partitioned parent.
    candidate : str
        The standalone table to attach.
    key_value : int
        The LIST value it will hold.
    schema : str, optional
        Schema of the partitioned parent. Defaults to the parent's own,
        else ``public``.
    partition_schema : str, optional
        Schema the child partitions live in. Defaults to the parent's,
        which is the layout PostgreSQL produces unless a schema is named.
    preflight : bool, default True
        Run :func:`~lightcurvedb.core.partitions.check_attachable` first
        and refuse on findings that would make the statement fail.
    allow_expensive : bool, default True
        Tolerate findings that make the attach slow rather than
        impossible. On by default because the expensive path is
        sometimes the only one available -- a partition of
        ``datasethierarchy`` cannot carry pre-validated foreign keys
        without blocking the very swap it is part of, so its attach is
        necessarily a clone-and-validate. Pass ``False`` where a purely
        catalog-level attach is the requirement.

    Raises
    ------
    NotAttachableError
        If the pre-flight found something disqualifying.
    """
    schema, parent = resolve_qualified_name(table, schema=schema)
    partition_schema = partition_schema or schema
    if preflight:
        check_attachable(
            conn,
            parent,
            candidate,
            key_value,
            schema=schema,
            partition_schema=partition_schema,
        ).raise_for_status(allow_expensive=allow_expensive)
    conn.execute(
        sa.text(
            f"ALTER TABLE {_qualified(conn, schema, parent)} "
            "ATTACH PARTITION "
            f"{_qualified(conn, partition_schema, candidate)} "
            f"FOR VALUES IN ({key_literal(key_value)})"
        )
    )


def detach_partition(
    conn: sa.Connection,
    table: TableRef,
    partition: str,
    *,
    schema: str | None = None,
    partition_schema: str | None = None,
    concurrently: bool = False,
    finalize: bool = False,
) -> None:
    """Detach ``partition`` from ``table``, leaving it standing.

    The detached relation keeps its rows, its primary key and its
    foreign keys, and becomes an ordinary table. Nothing is renamed, so
    which revision is live is always a question for ``pg_inherits``,
    never for the name.

    Parameters
    ----------
    conn : sqlalchemy.Connection
        Connection to emit on. Nothing is committed, except that
        ``concurrently=True`` requires an autocommit connection.
    table : str or sqlalchemy.Table or object with ``__table__``
        The partitioned parent.
    partition : str
        The attached partition to detach.
    schema : str, optional
        Schema of the partitioned parent. Defaults to the parent's own,
        else ``public``.
    partition_schema : str, optional
        Schema the child partitions live in. Defaults to the parent's,
        which is the layout PostgreSQL produces unless a schema is named.
    concurrently : bool, default False
        Use ``DETACH PARTITION ... CONCURRENTLY``, which takes only
        ``SHARE UPDATE EXCLUSIVE`` and so does not block readers.
    finalize : bool, default False
        Complete a concurrent detach that was interrupted, leaving the
        partition ``inhdetachpending``.

    Raises
    ------
    AutocommitRequiredError
        If ``concurrently=True`` on a connection that is not in
        autocommit. The check reads the connection's execution options:
        :meth:`~sqlalchemy.engine.Connection.get_isolation_level`
        reports the server's traditional isolation level even under
        autocommit, so it cannot answer this.
    ValueError
        If ``concurrently`` and ``finalize`` are both set.

    Notes
    -----
    A plain detach **adds no CHECK constraint** to the relation it
    leaves behind, so re-attaching it later means re-reading every row;
    follow it with :func:`add_bound_check` to keep a rollback cheap. A
    concurrent detach does add one -- and cannot be used at all while a
    DEFAULT partition exists on the parent, which PostgreSQL reports
    when the statement runs.

    Detaching a partition whose rows are still referenced by another
    table's foreign key is **blocked**, not silently allowed: PostgreSQL
    names the referencing relation in the error. Ordering the detaches
    referencing-side-first is therefore mandatory rather than merely
    tidy.
    """
    if concurrently and finalize:
        raise ValueError("concurrently and finalize are mutually exclusive")
    autocommit = conn.get_execution_options().get("isolation_level")
    if concurrently and autocommit != "AUTOCOMMIT":
        raise AutocommitRequiredError(
            "DETACH PARTITION ... CONCURRENTLY cannot run inside a "
            "transaction block; open the connection with "
            'execution_options(isolation_level="AUTOCOMMIT")'
        )
    schema, parent = resolve_qualified_name(table, schema=schema)
    partition_schema = partition_schema or schema
    suffix = ""
    if concurrently:
        suffix = " CONCURRENTLY"
    elif finalize:
        suffix = " FINALIZE"
    detached = _qualified(conn, partition_schema, partition)
    conn.execute(
        sa.text(
            f"ALTER TABLE {_qualified(conn, schema, parent)} "
            f"DETACH PARTITION {detached}{suffix}"
        )
    )


def drop_relation(
    conn: sa.Connection,
    relation: str,
    *,
    schema: str | None = None,
    cascade: bool = False,
) -> bool:
    """Drop a relation if it exists.

    Parameters
    ----------
    conn : sqlalchemy.Connection
        Connection to emit on. Nothing is committed.
    relation : str
        The relation to drop.
    schema : str, optional
        Schema to look in. Defaults to ``public``.
    cascade : bool, default False
        Drop dependent objects too.

    Returns
    -------
    bool
        Whether the relation existed.

    Notes
    -----
    This is a primitive and applies no policy: dropping an *attached*
    partition destroys live data. Retirement is gated in
    :func:`~lightcurvedb.core.partitions.swap.drop_retired`, which
    refuses anything still attached and is dry-run by default.
    """
    schema = schema or "public"
    if relation_kind(conn, relation, schema=schema) is None:
        return False
    tail = " CASCADE" if cascade else ""
    conn.execute(
        sa.text(f"DROP TABLE {_qualified(conn, schema, relation)}{tail}")
    )
    return True


# ---------------------------------------------------------------------------
# Locks and timeouts
# ---------------------------------------------------------------------------


def set_local_timeouts(
    conn: sa.Connection,
    *,
    lock_timeout: str | None = "5s",
    statement_timeout: str | None = "120s",
) -> None:
    """Bound both how long DDL waits and how long it holds.

    Setting only one of these is a trap. ``lock_timeout`` bounds how
    long a statement waits for a lock; ``statement_timeout`` bounds how
    long it runs once it has one. Without the second, a swap that queues
    behind a long-running reader eventually acquires ``ACCESS
    EXCLUSIVE`` and can then hold it indefinitely -- and every reader
    that arrived in the meantime is queued behind the swap.

    Parameters
    ----------
    conn : sqlalchemy.Connection
        Connection to set on. ``SET LOCAL`` lasts until the end of the
        caller's transaction and is rolled back with it.
    lock_timeout, statement_timeout : str or None
        PostgreSQL durations such as ``"5s"`` or ``"250ms"``. ``None``
        leaves that setting alone.

    Raises
    ------
    ValueError
        If a value is not a plain PostgreSQL duration.
    """
    for setting, value in (
        ("lock_timeout", lock_timeout),
        ("statement_timeout", statement_timeout),
    ):
        if value is None:
            continue
        if not _DURATION.match(value):
            raise ValueError(f"invalid {setting}: {value!r}")
        conn.execute(sa.text(f"SET LOCAL {setting} = '{value}'"))


def lock_tables(
    conn: sa.Connection,
    tables: Iterable[TableRef],
    mode: LockMode,
    *,
    schema: str | None = None,
) -> None:
    """Lock several tables in one statement.

    Taking every lock a transaction will need up front, in an order all
    actors agree on, is what makes contention fail fast instead of
    halfway through. One statement also means one wait against
    ``lock_timeout`` rather than one per table.

    Parameters
    ----------
    conn : sqlalchemy.Connection
        Connection to emit on. Locks are held to the end of the caller's
        transaction.
    tables : iterable
        Tables to lock, in the order given.
    mode : str
        A PostgreSQL lock mode, e.g. ``"ACCESS EXCLUSIVE"``.
    schema : str, optional
        Schema for tables that do not carry one. Defaults to ``public``.

    Raises
    ------
    ValueError
        If ``mode`` is not a PostgreSQL lock mode, or no tables given.
    """
    if mode not in _LOCK_MODES:
        raise ValueError(f"unknown lock mode: {mode!r}")
    names = [
        _qualified(conn, *resolve_qualified_name(table, schema=schema))
        for table in tables
    ]
    if not names:
        raise ValueError("no tables to lock")
    conn.execute(sa.text(f"LOCK TABLE {', '.join(names)} IN {mode} MODE"))


def referenced_tables(
    conn: sa.Connection,
    tables: Iterable[TableRef],
    *,
    schema: str | None = None,
) -> tuple[str, ...]:
    """The tables that ``tables`` point at with foreign keys.

    Attaching a partition touches the relations its foreign keys refer
    to, so they belong in the same up-front lock set as the parents. The
    result is sorted by name to give every actor the same order.

    Parameters
    ----------
    conn : sqlalchemy.Connection
        Connection to read with. Nothing is committed.
    tables : iterable
        Tables whose outbound keys to follow.
    schema : str, optional
        Schema for tables that do not carry one. Defaults to ``public``.

    Returns
    -------
    tuple of str
        Referenced relation names, sorted, excluding the inputs
        themselves and anything in another schema.
    """
    given = {resolve_qualified_name(table, schema=schema) for table in tables}
    found: set[str] = set()
    for table_schema, name in given:
        for spec in foreign_key_definitions(conn, name, schema=table_schema):
            pair = (spec.referenced_schema, spec.referenced_table)
            if pair in given or spec.referenced_schema != table_schema:
                continue
            found.add(spec.referenced_table)
    return tuple(sorted(found))
