"""Read-only introspection of PostgreSQL partitioning.

Everything here reads ``pg_catalog`` and nothing else. The catalog is the
structural truth about which relation is attached to which parent and
with what bound; no registry is consulted, so these answers cannot drift
from what the database will actually do.

Transaction contract
--------------------
Every function takes a :class:`sqlalchemy.Connection` and never commits.
All statements are reads, so calling these inside a caller's transaction
adds nothing to it beyond the reads themselves.

Tables may be named by string, by :class:`sqlalchemy.Table`, or by any
object carrying a ``__table__`` attribute. The last lets a mapped class be
passed in without this package importing :mod:`lightcurvedb.models`.
"""

from __future__ import annotations

import dataclasses
import re
from collections.abc import Mapping
from typing import Any, Final, Literal

import sqlalchemy as sa

from lightcurvedb.core.partitions.errors import (
    NotAttachableError,
    NotPartitionedError,
    RelationNotFoundError,
    UnsupportedPartitionStrategyError,
)

TableRef = Any
"""A table named by ``str``, :class:`sqlalchemy.Table`, or ``__table__``."""

PartitionStrategyName = Literal["list", "range", "hash"]

_STRATEGY_CODES: Final[dict[str, PartitionStrategyName]] = {
    "l": "list",
    "r": "range",
    "h": "hash",
}

_RELKIND_NAMES: Final[dict[str, str]] = {
    "r": "table",
    "p": "partitioned table",
    "i": "index",
    "I": "partitioned index",
    "v": "view",
    "m": "materialized view",
    "S": "sequence",
    "f": "foreign table",
}

_LIST_BOUND: Final = re.compile(r"^FOR VALUES IN \((.*)\)$")

# ``pg_get_indexdef`` renders ``CREATE [UNIQUE] INDEX <name> ON [ONLY]
# <table> USING <method> (<cols>) ...``. Everything after ``ON <table>`` is
# what makes two indexes structurally equivalent; ``ONLY`` appears for a
# partitioned parent's index and never for a plain table's.
_INDEXDEF: Final = re.compile(
    r"^CREATE (?P<unique>UNIQUE )?INDEX \S+ ON (?:ONLY )?\S+ (?P<body>.+)$"
)


# ---------------------------------------------------------------------------
# Name resolution
# ---------------------------------------------------------------------------


def resolve_table_name(table: TableRef) -> str:
    """Normalise a table reference to its bare relation name.

    Parameters
    ----------
    table : str, sqlalchemy.Table, or object with ``__table__``
        A mapped class qualifies through its ``__table__``.

    Raises
    ------
    TypeError
        If ``table`` is none of the accepted forms.
    """
    if isinstance(table, sa.Table):
        return table.name
    mapped = getattr(table, "__table__", None)
    if isinstance(mapped, sa.Table):
        return mapped.name
    if isinstance(table, str):
        return table
    raise TypeError(
        "expected a table name, sqlalchemy.Table or mapped class, got "
        + type(table).__name__
    )


def resolve_qualified_name(
    table: TableRef, *, schema: str | None = None
) -> tuple[str, str]:
    """Split a table reference into ``(schema, name)``.

    Every function in this package begins with this call, so that a
    mapped class, a :class:`sqlalchemy.Table` and a bare string all
    reach the catalog the same way.

    Parameters
    ----------
    table : str or sqlalchemy.Table or object with ``__table__``
        The relation to resolve.
    schema : str, optional
        Overrides the schema carried by ``table``. When neither supplies
        one, ``public`` is assumed.

    Returns
    -------
    tuple of (str, str)
        The schema and the relation name, neither quoted.
    """
    name = resolve_table_name(table)
    if schema is None:
        tbl = table if isinstance(table, sa.Table) else None
        if tbl is None:
            tbl = getattr(table, "__table__", None)
        schema = getattr(tbl, "schema", None) or "public"
    return schema, name


def _relation_oid(conn: sa.Connection, schema: str, name: str) -> int | None:
    return conn.execute(
        sa.text(
            "SELECT c.oid FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = :schema AND c.relname = :name"
        ),
        {"schema": schema, "name": name},
    ).scalar()


def _require_oid(conn: sa.Connection, schema: str, name: str) -> int:
    oid = _relation_oid(conn, schema, name)
    if oid is None:
        raise RelationNotFoundError(f"relation {schema}.{name} does not exist")
    return oid


def relation_kind(
    conn: sa.Connection, table: TableRef, *, schema: str | None = None
) -> str | None:
    """Describe what kind of relation a name refers to.

    Returns
    -------
    str or None
        ``"table"``, ``"partitioned table"``, ``"index"``, ``"view"`` and so
        on; the raw ``relkind`` letter for anything unrecognised; ``None``
        if no such relation exists.
    """
    schema, name = resolve_qualified_name(table, schema=schema)
    kind = conn.execute(
        sa.text(
            "SELECT c.relkind FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = :schema AND c.relname = :name"
        ),
        {"schema": schema, "name": name},
    ).scalar()
    if kind is None:
        return None
    return _RELKIND_NAMES.get(kind, kind)


# ---------------------------------------------------------------------------
# Partition strategy
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True, slots=True)
class PartitionStrategy:
    """How a partitioned table is partitioned.

    Attributes
    ----------
    strategy : {"list", "range", "hash"}
    key_columns : tuple of str
        Partition key columns in key order.
    default_partition_oid : int or None
        OID of the DEFAULT partition, if one is attached.
    """

    strategy: PartitionStrategyName
    key_columns: tuple[str, ...]
    default_partition_oid: int | None

    @property
    def key_column(self) -> str:
        """The single key column of a LIST-partitioned table.

        Raises
        ------
        UnsupportedPartitionStrategyError
            If there is more than one key column.
        """
        if len(self.key_columns) != 1:
            raise UnsupportedPartitionStrategyError(
                "expected a single partition key column, got "
                + repr(self.key_columns)
            )
        return self.key_columns[0]


def partition_strategy(
    conn: sa.Connection, table: TableRef, *, schema: str | None = None
) -> PartitionStrategy | None:
    """Report how ``table`` is partitioned.

    Returns
    -------
    PartitionStrategy or None
        ``None`` if the relation exists but is not partitioned.

    Raises
    ------
    RelationNotFoundError
        If the relation does not exist.
    UnsupportedPartitionStrategyError
        If any partition key is an expression rather than a column.
    """
    schema, name = resolve_qualified_name(table, schema=schema)
    oid = _require_oid(conn, schema, name)
    row = conn.execute(
        sa.text(
            "SELECT p.partstrat, p.partnatts, p.partdefid, "
            "  (SELECT array_agg(a.attname ORDER BY k.ord) "
            "     FROM unnest(p.partattrs::int2[]) "
            "          WITH ORDINALITY AS k(attnum, ord) "
            "     JOIN pg_attribute a "
            "       ON a.attrelid = p.partrelid AND a.attnum = k.attnum"
            "  ) AS key_columns "
            "FROM pg_partitioned_table p WHERE p.partrelid = :oid"
        ),
        {"oid": oid},
    ).one_or_none()
    if row is None:
        return None
    strat, natts, defid, key_columns = row
    key_columns = tuple(key_columns or ())
    if len(key_columns) != natts:
        # A partition key given as an expression has attnum 0 and no
        # pg_attribute row, so it drops out of the join.
        raise UnsupportedPartitionStrategyError(
            f"{schema}.{name} uses an expression as a partition key"
        )
    return PartitionStrategy(
        strategy=_STRATEGY_CODES[strat],
        key_columns=key_columns,
        default_partition_oid=defid or None,
    )


def require_list_partitioned(
    conn: sa.Connection, table: TableRef, *, schema: str | None = None
) -> PartitionStrategy:
    """Like :func:`partition_strategy`, but insist on LIST partitioning.

    Raises
    ------
    NotPartitionedError
        If the relation is not partitioned at all.
    UnsupportedPartitionStrategyError
        If it is partitioned by RANGE or HASH.
    """
    schema, name = resolve_qualified_name(table, schema=schema)
    strategy = partition_strategy(conn, name, schema=schema)
    if strategy is None:
        raise NotPartitionedError(f"{schema}.{name} is not partitioned")
    if strategy.strategy != "list":
        raise UnsupportedPartitionStrategyError(
            f"{schema}.{name} is {strategy.strategy.upper()}-partitioned"
            " -- only LIST is supported"
        )
    return strategy


# ---------------------------------------------------------------------------
# Partitions
# ---------------------------------------------------------------------------


def parse_list_bound(bound: str | None) -> tuple[int, ...]:
    """Extract the integer values from a LIST partition bound.

    Parameters
    ----------
    bound : str or None
        As rendered by ``pg_get_expr(relpartbound, oid)``: either
        ``"FOR VALUES IN (5)"``, ``"FOR VALUES IN (5, 6)"`` or
        ``"DEFAULT"``.

    Returns
    -------
    tuple of int
        Empty for the DEFAULT partition, for ``None``, and for any bound
        whose values are not all integers.
    """
    if not bound:
        return ()
    match = _LIST_BOUND.match(bound)
    if match is None:
        return ()
    try:
        return tuple(int(v.strip()) for v in match.group(1).split(","))
    except ValueError:
        return ()


@dataclasses.dataclass(frozen=True, slots=True)
class PartitionInfo:
    """One attached partition, as ``pg_catalog`` describes it.

    Attributes
    ----------
    oid, schema, name, parent
        Identity. ``parent`` is the partitioned table's bare name.
    bound : str or None
        Raw ``pg_get_expr(relpartbound, oid)`` text.
    list_values : tuple of int
        Parsed from ``bound``; empty for the DEFAULT partition.
    is_default : bool
    detach_pending : bool
        ``pg_inherits.inhdetachpending`` -- a ``DETACH CONCURRENTLY`` that
        did not complete. Fix with ``DETACH PARTITION ... FINALIZE``.
    row_estimate : int
        ``pg_class.reltuples``; ``-1`` when the relation has never been
        analysed or vacuumed.
    heap_bytes, index_bytes, total_bytes : int
        ``pg_relation_size``, ``pg_indexes_size`` and
        ``pg_total_relation_size``. Only ``total_bytes`` includes TOAST,
        which for array-heavy tables is most of the storage.
    """

    oid: int
    schema: str
    name: str
    parent: str
    bound: str | None
    list_values: tuple[int, ...]
    is_default: bool
    detach_pending: bool
    row_estimate: int
    heap_bytes: int
    index_bytes: int
    total_bytes: int

    @property
    def toast_bytes(self) -> int:
        """Out-of-line storage: total less heap less indexes."""
        return self.total_bytes - self.heap_bytes - self.index_bytes

    @property
    def qualified_name(self) -> str:
        return f"{self.schema}.{self.name}"


def list_table_partitions(
    conn: sa.Connection, table: TableRef, *, schema: str | None = None
) -> list[PartitionInfo]:
    """Every partition directly attached to ``table``, by name.

    Raises
    ------
    RelationNotFoundError
        If ``table`` does not exist.
    """
    schema, name = resolve_qualified_name(table, schema=schema)
    parent_oid = _require_oid(conn, schema, name)
    rows = conn.execute(
        sa.text(
            "SELECT c.oid, n.nspname, c.relname, "
            "       pg_get_expr(c.relpartbound, c.oid) AS bound, "
            "       i.inhdetachpending, "
            "       c.reltuples::bigint AS row_estimate, "
            "       pg_relation_size(c.oid) AS heap_bytes, "
            "       pg_indexes_size(c.oid) AS index_bytes, "
            "       pg_total_relation_size(c.oid) AS total_bytes "
            "FROM pg_inherits i "
            "JOIN pg_class c ON c.oid = i.inhrelid "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE i.inhparent = :parent AND c.relkind IN ('r', 'p') "
            "ORDER BY c.relname"
        ),
        {"parent": parent_oid},
    ).all()
    return [
        PartitionInfo(
            oid=r.oid,
            schema=r.nspname,
            name=r.relname,
            parent=name,
            bound=r.bound,
            list_values=parse_list_bound(r.bound),
            is_default=r.bound == "DEFAULT",
            detach_pending=bool(r.inhdetachpending),
            row_estimate=r.row_estimate,
            heap_bytes=r.heap_bytes,
            index_bytes=r.index_bytes,
            total_bytes=r.total_bytes,
        )
        for r in rows
    ]


def find_partition_for_value(
    conn: sa.Connection,
    table: TableRef,
    value: int,
    *,
    schema: str | None = None,
) -> PartitionInfo | None:
    """The non-default partition whose LIST bound contains ``value``.

    The DEFAULT partition is never returned; use
    :func:`default_partition_of` to ask about it explicitly.
    """
    for info in list_table_partitions(conn, table, schema=schema):
        if not info.is_default and value in info.list_values:
            return info
    return None


def default_partition_of(
    conn: sa.Connection, table: TableRef, *, schema: str | None = None
) -> PartitionInfo | None:
    """The DEFAULT partition of ``table``, if one is attached."""
    for info in list_table_partitions(conn, table, schema=schema):
        if info.is_default:
            return info
    return None


# ---------------------------------------------------------------------------
# Column signatures
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True, slots=True)
class ColumnSpec:
    """One column of a relation, as PostgreSQL reports it.

    Attributes
    ----------
    name : str
        Column name.
    type_name : str
        Type as ``format_type`` renders it, so ``bigint`` and
        ``double precision[]`` compare as written rather than by OID.
    not_null : bool
        Whether the column carries ``NOT NULL``.
    ordinal : int
        Position among the live columns, counting from 1.

        Deliberately not the raw ``attnum``. Dropping a column leaves a
        hole in a table's ``attnum`` sequence, but ``CREATE TABLE ...
        LIKE`` numbers the copy contiguously, so comparing raw values
        would report every column after the hole as misplaced on a
        staging table that is in fact correct.
    """

    name: str
    type_name: str
    not_null: bool
    ordinal: int


def column_signature(
    conn: sa.Connection, table: TableRef, *, schema: str | None = None
) -> dict[str, ColumnSpec]:
    """Describe every live column of ``table``, keyed by name.

    Parameters
    ----------
    conn : sqlalchemy.Connection
        Connection to read with. Nothing is committed.
    table : str or sqlalchemy.Table or object with ``__table__``
        Relation to describe.
    schema : str, optional
        Schema to look in. Defaults to the table's own schema, else
        ``public``.

    Returns
    -------
    dict of str to ColumnSpec
        System and dropped columns are omitted.

    Raises
    ------
    RelationNotFoundError
        If the relation does not exist.
    """
    schema, name = resolve_qualified_name(table, schema=schema)
    return _columns(conn, _require_oid(conn, schema, name))


def column_mismatches(
    parent: Mapping[str, ColumnSpec],
    candidate: Mapping[str, ColumnSpec],
    *,
    compare_ordinals: bool = False,
) -> tuple[str, ...]:
    """Differences between two :func:`column_signature` results.

    Parameters
    ----------
    parent, candidate : mapping of str to ColumnSpec
        Signatures to compare. Findings are phrased from the parent's
        point of view.
    compare_ordinals : bool, default False
        Also report columns whose physical position differs.
        ``ATTACH PARTITION`` matches columns by name and is indifferent
        to their order, so this stays off for attachability; it is worth
        turning on to prove a staging table was built from the parent's
        current definition rather than an older one.

    Returns
    -------
    tuple of str
        One human-readable finding per difference, empty when the two
        agree.
    """
    found: list[str] = []
    for col, spec in parent.items():
        other = candidate.get(col)
        if other is None:
            found.append(f"{col} missing from candidate")
            continue
        if spec.type_name != other.type_name:
            found.append(
                f"{col} is {spec.type_name} on parent, "
                f"{other.type_name} on candidate"
            )
        elif spec.not_null and not other.not_null:
            found.append(f"{col} is NOT NULL on parent, nullable on candidate")
        if compare_ordinals and spec.ordinal != other.ordinal:
            found.append(
                f"{col} is column {spec.ordinal} on parent, "
                f"{other.ordinal} on candidate"
            )
    found.extend(
        f"{col} on candidate but not on parent"
        for col in candidate
        if col not in parent
    )
    return tuple(found)


def _columns(conn: sa.Connection, oid: int) -> dict[str, ColumnSpec]:
    rows = conn.execute(
        sa.text(
            "SELECT a.attname AS name, "
            "       format_type(a.atttypid, a.atttypmod) AS type_name, "
            "       a.attnotnull AS not_null, "
            "       row_number() OVER (ORDER BY a.attnum) AS ordinal "
            "FROM pg_attribute a "
            "WHERE a.attrelid = :oid AND a.attnum > 0 AND NOT a.attisdropped "
            "ORDER BY a.attnum"
        ),
        {"oid": oid},
    ).all()
    return {
        r.name: ColumnSpec(r.name, r.type_name, bool(r.not_null), r.ordinal)
        for r in rows
    }


# ---------------------------------------------------------------------------
# Indexes and foreign keys
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True, slots=True)
class IndexSpec:
    """One index on a relation, with the constraint behind it if any.

    Attributes
    ----------
    name : str
        Index relation name.
    definition : str
        ``pg_get_indexdef`` output, naming the index and its table.
    valid : bool
        ``indisvalid``. A false one is debris from a failed
        ``CREATE INDEX CONCURRENTLY`` and is ignored by the planner.
    constraint_type : str or None
        ``p`` when a PRIMARY KEY stands behind this index, ``u`` for a
        UNIQUE constraint, ``None`` for a plain index.
    constraint_name : str or None
        Name of that constraint.
    constraint_definition : str or None
        ``pg_get_constraintdef`` output for it, ready to re-emit after
        ``ADD CONSTRAINT``.

    Notes
    -----
    Whether an index is constraint-backed decides what ``ATTACH
    PARTITION`` does with it. ``AttachPartitionEnsureIndexes`` adopts a
    child index only when the parent's is constraint-backed and the
    child's is too; a matching-but-constraintless unique index is
    rejected and rebuilt while ``ACCESS EXCLUSIVE`` is held.
    """

    name: str
    definition: str
    valid: bool
    constraint_type: str | None
    constraint_name: str | None
    constraint_definition: str | None

    @property
    def constraint_backed(self) -> bool:
        """Whether a PRIMARY KEY or UNIQUE constraint owns this index."""
        return self.constraint_type is not None

    @property
    def key(self) -> str:
        """The definition stripped of index and table names.

        Two indexes with the same key are structurally the same index on
        different relations, which is what makes a parent's index set
        comparable with a candidate's.
        """
        match = _INDEXDEF.match(self.definition)
        if match is None:
            return self.definition
        return (match.group("unique") or "") + match.group("body")

    @property
    def unique(self) -> bool:
        """Whether the index enforces uniqueness."""
        match = _INDEXDEF.match(self.definition)
        return bool(match and match.group("unique"))

    @property
    def body(self) -> str | None:
        """Everything after the table name, ``USING`` onwards.

        Re-emitting this under a new index name on a new table
        reproduces the index exactly, including operator classes,
        collations, ``INCLUDE`` columns and partial predicates, none of
        which survive being rebuilt from a column list.
        """
        match = _INDEXDEF.match(self.definition)
        return None if match is None else match.group("body")


@dataclasses.dataclass(frozen=True, slots=True)
class ForeignKeySpec:
    """One outbound foreign key, with what it points at.

    Attributes
    ----------
    name : str
        Constraint name. Per-partition names are auto-generated,
        truncated and numerically suffixed, so never hardcode one.
    definition : str
        ``pg_get_constraintdef`` output, re-emittable verbatim after
        ``ADD CONSTRAINT``.
    validated : bool
        ``convalidated``. Only a validated constraint can be adopted at
        attach time instead of re-checked.
    columns : tuple of str
        The constrained columns, in key order.
    referenced_schema, referenced_table : str
        The relation this key points at.
    referenced_columns : tuple of str
        The columns it points at, in the same order as ``columns``.
    referenced_is_partitioned : bool
        Whether that relation is itself partitioned. Mirroring such a
        key onto a staging table makes the staging table pin the
        referent's current partitions, which is what blocks a swap.
    """

    name: str
    definition: str
    validated: bool
    columns: tuple[str, ...]
    referenced_schema: str
    referenced_table: str
    referenced_columns: tuple[str, ...]
    referenced_is_partitioned: bool


def index_definitions(
    conn: sa.Connection, table: TableRef, *, schema: str | None = None
) -> tuple[IndexSpec, ...]:
    """Every index on ``table``, ordered by name.

    Parameters
    ----------
    conn : sqlalchemy.Connection
        Connection to read with. Nothing is committed.
    table : str or sqlalchemy.Table or object with ``__table__``
        Relation to inspect.
    schema : str, optional
        Schema to look in. Defaults to the table's own schema, else
        ``public``.

    Returns
    -------
    tuple of IndexSpec

    Raises
    ------
    RelationNotFoundError
        If the relation does not exist.
    """
    schema, name = resolve_qualified_name(table, schema=schema)
    return _index_specs(conn, _require_oid(conn, schema, name))


def foreign_key_definitions(
    conn: sa.Connection, table: TableRef, *, schema: str | None = None
) -> tuple[ForeignKeySpec, ...]:
    """Every outbound foreign key on ``table``, ordered by name.

    Inbound keys -- other tables referencing this one -- are not
    reported.

    Parameters
    ----------
    conn : sqlalchemy.Connection
        Connection to read with. Nothing is committed.
    table : str or sqlalchemy.Table or object with ``__table__``
        Relation to inspect.
    schema : str, optional
        Schema to look in. Defaults to the table's own schema, else
        ``public``.

    Returns
    -------
    tuple of ForeignKeySpec

    Raises
    ------
    RelationNotFoundError
        If the relation does not exist.
    """
    schema, name = resolve_qualified_name(table, schema=schema)
    return _foreign_key_specs(conn, _require_oid(conn, schema, name))


# ---------------------------------------------------------------------------
# Attachability pre-flight
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True, slots=True)
class AttachabilityReport:
    """What ``ATTACH PARTITION`` would do with a candidate table.

    Findings are split by consequence. :attr:`blocking` findings make the
    ``ATTACH`` fail. :attr:`expensive` findings let it succeed but force
    PostgreSQL to scan or build something while holding ``ACCESS
    EXCLUSIVE`` on the parent -- the outage the pre-flight exists to
    prevent.

    Missing foreign keys are reported, not prescribed: on a table whose
    foreign keys reference the parent being swapped, pre-creating them
    pins the outgoing partition and blocks the swap. The workflow layer
    decides which side each table is on.

    Attributes
    ----------
    parent_schema, candidate_schema : str
        Where each relation lives. They differ when a deployment keeps
        its partitions in a schema of their own, so every message naming
        a relation qualifies it -- see :attr:`qualified_parent` and
        :attr:`qualified_candidate`.
    column_mismatches : tuple of str
        Columns missing, extra, mistyped, or nullable where the parent's
        is ``NOT NULL``. Blocking.
    missing_indexes : tuple of str
        Parent indexes with no structural match on the candidate.
        PostgreSQL builds them during ``ATTACH``. Expensive.
    invalid_indexes : tuple of str
        Candidate indexes with ``indisvalid = false``. Expensive.
    unbacked_constraint_indexes : tuple of str
        Parent indexes backing a PRIMARY KEY or UNIQUE constraint whose
        candidate match is a bare index. PostgreSQL will not adopt it and
        rebuilds instead. Expensive.
    has_valid_partition_check : bool
        Whether a *validated* ``CHECK (<key> = <value>)`` exists. Without
        it ``ATTACH`` scans every row to prove the bound. Expensive if
        absent.
    missing_foreign_keys : tuple of str
        Parent foreign keys with no validated equivalent on the candidate.
        PostgreSQL clones and validates them during ``ATTACH``. Expensive.
    default_partition : str or None
        Name of the parent's DEFAULT partition, if any. Its presence alone
        is expensive: ``ATTACH`` scans it under ``ACCESS EXCLUSIVE``.
    default_partition_conflicts : bool
        Whether the DEFAULT partition holds rows for ``key_value``.
        Blocking -- ``ATTACH`` fails outright.
    """

    parent: str
    parent_schema: str
    candidate: str
    candidate_schema: str
    key_column: str
    key_value: int
    candidate_kind: str | None
    column_mismatches: tuple[str, ...]
    missing_indexes: tuple[str, ...]
    invalid_indexes: tuple[str, ...]
    unbacked_constraint_indexes: tuple[str, ...]
    has_valid_partition_check: bool
    missing_foreign_keys: tuple[str, ...]
    default_partition: str | None
    default_partition_conflicts: bool

    @property
    def qualified_parent(self) -> str:
        """``<parent_schema>.<parent>``."""
        return f"{self.parent_schema}.{self.parent}"

    @property
    def qualified_candidate(self) -> str:
        """``<candidate_schema>.<candidate>``."""
        return f"{self.candidate_schema}.{self.candidate}"

    @property
    def blocking(self) -> tuple[str, ...]:
        """Findings that would make ``ATTACH`` fail."""
        found: list[str] = []
        if self.candidate_kind != "table":
            found.append(
                f"candidate {self.qualified_candidate} is "
                + (self.candidate_kind or "absent")
                + ", not a plain table"
            )
        found.extend(f"column: {m}" for m in self.column_mismatches)
        if self.default_partition_conflicts:
            found.append(
                f"default partition {self.default_partition} holds rows "
                f"where {self.key_column} = {self.key_value}"
            )
        return tuple(found)

    @property
    def expensive(self) -> tuple[str, ...]:
        """Findings that would make ``ATTACH`` scan or build under lock."""
        found: list[str] = []
        found.extend(f"missing index: {i}" for i in self.missing_indexes)
        found.extend(f"invalid index: {i}" for i in self.invalid_indexes)
        found.extend(
            f"index not constraint-backed: {i}"
            for i in self.unbacked_constraint_indexes
        )
        if not self.has_valid_partition_check:
            found.append(
                f"no validated CHECK ({self.key_column} = {self.key_value})"
                " -- ATTACH will scan every row"
            )
        found.extend(
            f"missing foreign key: {fk}" for fk in self.missing_foreign_keys
        )
        if self.default_partition and not self.default_partition_conflicts:
            found.append(
                f"default partition {self.default_partition} exists -- "
                "ATTACH will scan it"
            )
        return tuple(found)

    @property
    def ok(self) -> bool:
        """True when ``ATTACH`` would be a pure catalog operation."""
        return not self.blocking and not self.expensive

    def raise_for_status(self, *, allow_expensive: bool = False) -> None:
        """Raise unless the attach is clean.

        Parameters
        ----------
        allow_expensive : bool, optional
            Tolerate findings that only slow the attach. Blocking findings
            always raise.

        Raises
        ------
        NotAttachableError
        """
        problems = list(self.blocking)
        if not allow_expensive:
            problems.extend(self.expensive)
        if problems:
            raise NotAttachableError(
                f"{self.qualified_candidate} cannot be attached to "
                f"{self.qualified_parent} "
                f"for {self.key_column} = {self.key_value}"
                + ":\n  "
                + "\n  ".join(problems)
            )


def _index_specs(conn: sa.Connection, oid: int) -> tuple[IndexSpec, ...]:
    rows = conn.execute(
        sa.text(
            "SELECT ic.relname AS name, "
            "       pg_get_indexdef(i.indexrelid) AS definition, "
            "       i.indisvalid AS valid, "
            "       k.contype AS constraint_type, "
            "       k.conname AS constraint_name, "
            "       CASE WHEN k.oid IS NULL THEN NULL "
            "            ELSE pg_get_constraintdef(k.oid) END "
            "         AS constraint_definition "
            "FROM pg_index i "
            "JOIN pg_class ic ON ic.oid = i.indexrelid "
            "LEFT JOIN pg_constraint k ON k.conindid = i.indexrelid "
            "     AND k.contype IN ('p', 'u') "
            "WHERE i.indrelid = :oid ORDER BY ic.relname"
        ),
        {"oid": oid},
    ).all()
    return tuple(
        IndexSpec(
            name=r.name,
            definition=r.definition,
            valid=bool(r.valid),
            constraint_type=r.constraint_type,
            constraint_name=r.constraint_name,
            constraint_definition=r.constraint_definition,
        )
        for r in rows
    )


def _foreign_key_specs(
    conn: sa.Connection, oid: int
) -> tuple[ForeignKeySpec, ...]:
    rows = conn.execute(
        sa.text(
            "SELECT c.conname AS name, "
            "       pg_get_constraintdef(c.oid) AS definition, "
            "       c.convalidated AS validated, "
            "       (SELECT array_agg(a.attname ORDER BY k.ord) "
            "          FROM unnest(c.conkey) WITH ORDINALITY AS k(num, ord) "
            "          JOIN pg_attribute a ON a.attrelid = c.conrelid "
            "               AND a.attnum = k.num) AS columns, "
            "       rn.nspname AS referenced_schema, "
            "       rc.relname AS referenced_table, "
            "       (SELECT array_agg(a.attname ORDER BY k.ord) "
            "          FROM unnest(c.confkey) WITH ORDINALITY AS k(num, ord) "
            "          JOIN pg_attribute a ON a.attrelid = c.confrelid "
            "               AND a.attnum = k.num) AS referenced_columns, "
            "       (rc.relkind = 'p') AS referenced_is_partitioned "
            "FROM pg_constraint c "
            "JOIN pg_class rc ON rc.oid = c.confrelid "
            "JOIN pg_namespace rn ON rn.oid = rc.relnamespace "
            "WHERE c.conrelid = :oid AND c.contype = 'f' "
            "ORDER BY c.conname"
        ),
        {"oid": oid},
    ).all()
    return tuple(
        ForeignKeySpec(
            name=r.name,
            definition=r.definition,
            validated=bool(r.validated),
            columns=tuple(r.columns or ()),
            referenced_schema=r.referenced_schema,
            referenced_table=r.referenced_table,
            referenced_columns=tuple(r.referenced_columns or ()),
            referenced_is_partitioned=bool(r.referenced_is_partitioned),
        )
        for r in rows
    )


def _has_valid_partition_check(
    conn: sa.Connection, oid: int, key_column: str, key_value: int
) -> bool:
    rows = conn.execute(
        sa.text(
            "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
            "WHERE conrelid = :oid AND contype = 'c' AND convalidated"
        ),
        {"oid": oid},
    ).scalars()
    pattern = re.compile(
        r"\b" + re.escape(key_column) + r"\s*=\s*" + str(key_value) + r"\b"
    )
    return any(pattern.search(d) for d in rows)


def _default_holds(
    conn: sa.Connection,
    schema: str,
    default_name: str,
    key_column: str,
    key_value: int,
) -> bool:
    quote = conn.dialect.identifier_preparer.quote
    relation = f"{quote(schema)}.{quote(default_name)}"
    return bool(
        conn.execute(
            sa.text(
                f"SELECT EXISTS (SELECT 1 FROM {relation} "
                f"WHERE {quote(key_column)} = "
                ":value)"
            ),
            {"value": key_value},
        ).scalar()
    )


def has_bound_check(
    conn: sa.Connection,
    relation: str,
    key_column: str,
    key_value: int,
    *,
    schema: str | None = None,
) -> bool:
    """Whether a relation carries a validated ``<key> = <value>`` CHECK.

    Such a constraint is what lets ``ATTACH PARTITION`` skip reading
    every row. It is worth knowing about a *detached* relation too: one
    without it can still be re-attached, but only by paying for a full
    scan while the parent is locked.

    Parameters
    ----------
    conn : sqlalchemy.Connection
        Connection to read with. Nothing is committed.
    relation : str
        The relation to inspect.
    key_column : str
        The parent's partition key column.
    key_value : int
        The value the relation should be constrained to.
    schema : str, optional
        Schema ``relation`` lives in -- the partition schema, which is
        not necessarily the parent's. Defaults to ``public``.

    Returns
    -------
    bool
        False if the relation does not exist.

    Notes
    -----
    Only validated constraints count. ``ConstraintImpliedByRelConstraint``
    skips anything ``NOT VALID``, so an unvalidated CHECK is invisible
    to the prover and buys nothing.
    """
    schema = schema or "public"
    oid = _relation_oid(conn, schema, relation)
    if oid is None:
        return False
    return _has_valid_partition_check(conn, oid, key_column, key_value)


def check_attachable(
    conn: sa.Connection,
    table: TableRef,
    candidate: str,
    key_value: int,
    *,
    schema: str | None = None,
    partition_schema: str | None = None,
) -> AttachabilityReport:
    """Predict what ``ATTACH PARTITION candidate FOR VALUES IN (key_value)``
    would do, without taking any lock.

    Compares the candidate against the parent on columns, indexes,
    constraint-backed indexes, the bound-implying ``CHECK``, foreign keys,
    and the DEFAULT partition. The one non-catalog read is an existence
    probe into the DEFAULT partition for ``key_value``; on a large default
    that is a real scan, but it is a plain read rather than one under
    ``ACCESS EXCLUSIVE``.

    ``schema`` names the parent; ``partition_schema`` names where
    ``candidate`` lives, and defaults to the parent's schema, which is the
    layout PostgreSQL produces unless a schema is named. The DEFAULT
    partition is found by oid and probed wherever it actually is, so it
    needs neither.

    Raises
    ------
    RelationNotFoundError
        If the parent does not exist. A missing *candidate* is reported in
        the returned report rather than raised, so a caller can ask about
        a table it has not created yet.
    NotPartitionedError, UnsupportedPartitionStrategyError
        If the parent is not LIST-partitioned on a single column.
    """
    schema, parent = resolve_qualified_name(table, schema=schema)
    partition_schema = partition_schema or schema
    parent_oid = _require_oid(conn, schema, parent)
    strategy = require_list_partitioned(conn, parent, schema=schema)
    key_column = strategy.key_column

    candidate_kind = relation_kind(conn, candidate, schema=partition_schema)
    candidate_oid = _relation_oid(conn, partition_schema, candidate)

    default_name: str | None = None
    default_conflicts = False
    if strategy.default_partition_oid is not None:
        # The DEFAULT partition is a child, and a child need not live in
        # its parent's schema. Read where it actually is rather than
        # re-qualifying its name with the parent's.
        default_row = conn.execute(
            sa.text(
                "SELECT n.nspname, c.relname FROM pg_class c "
                "JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE c.oid = :oid"
            ),
            {"oid": strategy.default_partition_oid},
        ).one_or_none()
        if default_row is not None:
            found: str = default_row.relname
            default_name = found
            default_conflicts = _default_holds(
                conn,
                default_row.nspname,
                found,
                key_column,
                key_value,
            )

    if candidate_oid is None or candidate_kind != "table":
        return AttachabilityReport(
            parent=parent,
            parent_schema=schema,
            candidate=candidate,
            candidate_schema=partition_schema,
            key_column=key_column,
            key_value=key_value,
            candidate_kind=candidate_kind,
            column_mismatches=(),
            missing_indexes=(),
            invalid_indexes=(),
            unbacked_constraint_indexes=(),
            has_valid_partition_check=False,
            missing_foreign_keys=(),
            default_partition=default_name,
            default_partition_conflicts=default_conflicts,
        )

    parent_indexes = _index_specs(conn, parent_oid)
    candidate_indexes = _index_specs(conn, candidate_oid)
    candidate_by_key = {idx.key: idx for idx in candidate_indexes}
    missing: list[str] = []
    unbacked: list[str] = []
    for pidx in parent_indexes:
        match = candidate_by_key.get(pidx.key)
        if match is None:
            missing.append(pidx.name)
        elif pidx.constraint_backed and not match.constraint_backed:
            unbacked.append(match.name)

    candidate_fks = {
        fk.definition
        for fk in _foreign_key_specs(conn, candidate_oid)
        if fk.validated
    }
    missing_fks = tuple(
        fk.name + ": " + fk.definition
        for fk in _foreign_key_specs(conn, parent_oid)
        if fk.definition not in candidate_fks
    )

    return AttachabilityReport(
        parent=parent,
        parent_schema=schema,
        candidate=candidate,
        candidate_schema=partition_schema,
        key_column=key_column,
        key_value=key_value,
        candidate_kind=candidate_kind,
        column_mismatches=column_mismatches(
            _columns(conn, parent_oid), _columns(conn, candidate_oid)
        ),
        missing_indexes=tuple(missing),
        invalid_indexes=tuple(
            idx.name for idx in candidate_indexes if not idx.valid
        ),
        unbacked_constraint_indexes=tuple(unbacked),
        has_valid_partition_check=_has_valid_partition_check(
            conn, candidate_oid, key_column, key_value
        ),
        missing_foreign_keys=missing_fks,
        default_partition=default_name,
        default_partition_conflicts=default_conflicts,
    )
