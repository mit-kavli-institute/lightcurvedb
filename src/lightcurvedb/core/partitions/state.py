"""Where a revision is in its lifecycle, derived from the catalog.

A revision moves ABSENT -> CREATED -> LOADED -> INDEXED -> FK_READY ->
VERIFIED -> LIVE, and afterwards LIVE -> RETIRED -> GONE. Nothing here
stores that state: it is re-derived from ``pg_catalog`` every time, so a
process that dies mid-campaign learns where it got to by looking, and a
partition created by hand years ago is understood without having been
recorded anywhere.

Three of the states are not physical and cannot be derived here.
``VERIFIED`` records that a human looked at the data; ``RETIRED``
differs from a fully prepared staging table by history alone -- a
detached relation is byte-for-byte what it was while attached; and
``GONE`` differs from ``ABSENT`` only in that the relation once
existed. Those three come from the registry in
:mod:`lightcurvedb.models.partition_revision`, which exists precisely
because the catalog cannot answer them.

Transaction contract
--------------------
Every function takes a :class:`sqlalchemy.Connection` and never
commits. All statements are reads.
"""

from __future__ import annotations

import enum
from collections.abc import Iterable
from typing import Final

import sqlalchemy as sa

from lightcurvedb.core.partitions.catalog import (
    TableRef,
    find_partition_for_value,
    foreign_key_definitions,
    index_definitions,
    relation_kind,
    resolve_qualified_name,
)
from lightcurvedb.core.partitions.errors import (
    IllegalStateTransitionError,
    PartitionNameError,
    RevisionSkewError,
)
from lightcurvedb.core.partitions.naming import PartitionName


class RevisionState(enum.Enum):
    """Where one revision of one slot has got to.

    Members
    -------
    ABSENT
        No relation by that name exists.
    CREATED
        The staging table exists and is empty.
    LOADED
        It holds rows but has no indexes yet.
    INDEXED
        Its index set matches the parent's.
    FK_READY
        Every foreign key that can be pre-created has been, so an
        attach would adopt rather than re-validate them.
    VERIFIED
        Someone has compared it against the live data and accepted it.
        Registry-only.
    LIVE
        Attached to the parent and serving queries.
    DETACH_PENDING
        A concurrent detach was interrupted and needs ``FINALIZE``.
    RETIRED
        Detached after having been live, still on disk and still
        re-attachable. Registry-only.
    GONE
        Dropped. Registry-only.
    """

    ABSENT = "absent"
    CREATED = "created"
    LOADED = "loaded"
    INDEXED = "indexed"
    FK_READY = "fk_ready"
    VERIFIED = "verified"
    LIVE = "live"
    DETACH_PENDING = "detach_pending"
    RETIRED = "retired"
    GONE = "gone"


#: The only moves allowed between states. Staying put is always legal
#: and is not listed. Abandoning a staged revision -- dropping it before
#: it ever went live -- is the edge back to ``ABSENT``.
LEGAL_TRANSITIONS: Final[dict[RevisionState, frozenset[RevisionState]]] = {
    RevisionState.ABSENT: frozenset({RevisionState.CREATED}),
    RevisionState.CREATED: frozenset(
        {RevisionState.LOADED, RevisionState.ABSENT}
    ),
    RevisionState.LOADED: frozenset(
        {RevisionState.INDEXED, RevisionState.ABSENT}
    ),
    RevisionState.INDEXED: frozenset(
        {RevisionState.FK_READY, RevisionState.VERIFIED, RevisionState.ABSENT}
    ),
    RevisionState.FK_READY: frozenset(
        {RevisionState.VERIFIED, RevisionState.LIVE, RevisionState.ABSENT}
    ),
    RevisionState.VERIFIED: frozenset(
        {RevisionState.LIVE, RevisionState.ABSENT}
    ),
    RevisionState.LIVE: frozenset(
        {RevisionState.RETIRED, RevisionState.DETACH_PENDING}
    ),
    RevisionState.DETACH_PENDING: frozenset({RevisionState.RETIRED}),
    RevisionState.RETIRED: frozenset({RevisionState.LIVE, RevisionState.GONE}),
    RevisionState.GONE: frozenset(),
}


def validate_transition(current: RevisionState, target: RevisionState) -> None:
    """Refuse a move the lifecycle does not allow.

    Parameters
    ----------
    current, target : RevisionState
        Where the revision is and where it is being moved to. Moving a
        revision to the state it is already in is always allowed, so
        that recording the same step twice is a no-op rather than an
        error.

    Raises
    ------
    IllegalStateTransitionError
        If no edge joins the two.
    """
    if current is target:
        return
    if target not in LEGAL_TRANSITIONS[current]:
        allowed = ", ".join(
            sorted(s.value for s in LEGAL_TRANSITIONS[current])
        )
        raise IllegalStateTransitionError(
            f"cannot go from {current.value} to {target.value}. "
            f"Allowed: {allowed or 'nothing, it is terminal'}"
        )


def derive_state(
    conn: sa.Connection,
    table: TableRef,
    key_value: int,
    revision: int,
    *,
    schema: str | None = None,
) -> RevisionState:
    """Read one revision's physical state out of ``pg_catalog``.

    Parameters
    ----------
    conn : sqlalchemy.Connection
        Connection to read with. Nothing is committed.
    table : str or sqlalchemy.Table or object with ``__table__``
        The partitioned parent.
    key_value : int
        The LIST value.
    revision : int
        Which revision to ask about.
    schema : str, optional
        Schema to look in. Defaults to the parent's, else ``public``.

    Returns
    -------
    RevisionState
        One of ``ABSENT``, ``CREATED``, ``LOADED``, ``INDEXED``,
        ``FK_READY``, ``LIVE`` or ``DETACH_PENDING``. Never
        ``VERIFIED``, ``RETIRED`` or ``GONE`` -- see the module
        docstring for why those cannot be derived.

    Notes
    -----
    Emptiness separates ``CREATED`` from ``LOADED`` and nothing else.
    A revision can legitimately hold no rows -- a reprocessing that
    produces no lineage, or an observation with nothing in it -- and
    such a revision still reaches ``FK_READY`` and is still promotable.
    The catalog cannot tell "the load has not run" from "the load ran
    and produced nothing", so preparation is judged by the indexes and
    keys that are present rather than by row count.

    ``FK_READY`` counts only the foreign keys that *can* be pre-created:
    those pointing at unpartitioned tables. A key pointing at a
    partitioned table is deliberately never mirrored onto a staging
    table, because doing so pins the partition being replaced and blocks
    the swap, so its absence is not an unfinished step.
    """
    schema, parent = resolve_qualified_name(table, schema=schema)
    name = PartitionName(parent, key_value, revision).table

    if relation_kind(conn, name, schema=schema) is None:
        return RevisionState.ABSENT

    live = find_partition_for_value(conn, parent, key_value, schema=schema)
    if live is not None and live.name == name:
        return (
            RevisionState.DETACH_PENDING
            if live.detach_pending
            else RevisionState.LIVE
        )

    parent_indexes = {
        spec.key for spec in index_definitions(conn, parent, schema=schema)
    }
    candidate_indexes = {
        spec.key for spec in index_definitions(conn, name, schema=schema)
    }

    if parent_indexes <= candidate_indexes:
        wanted = {
            spec.definition
            for spec in foreign_key_definitions(conn, parent, schema=schema)
            if not spec.referenced_is_partitioned
        }
        present = {
            spec.definition
            for spec in foreign_key_definitions(conn, name, schema=schema)
            if spec.validated
        }
        if wanted <= present:
            return RevisionState.FK_READY
        return RevisionState.INDEXED

    if _has_rows(conn, schema, name):
        return RevisionState.LOADED
    return RevisionState.CREATED


def live_revision(
    conn: sa.Connection,
    table: TableRef,
    key_value: int,
    *,
    schema: str | None = None,
) -> int | None:
    """Which revision is currently attached for ``key_value``.

    Parameters
    ----------
    conn : sqlalchemy.Connection
        Connection to read with. Nothing is committed.
    table : str or sqlalchemy.Table or object with ``__table__``
        The partitioned parent.
    key_value : int
        The LIST value.
    schema : str, optional
        Schema to look in. Defaults to the parent's, else ``public``.

    Returns
    -------
    int or None
        The live revision, or ``None`` if the slot is unprovisioned. A
        partition named by hand as ``<base>_obs_<id>`` reads as
        revision 0.

    Raises
    ------
    PartitionNameError
        If the attached partition's name does not follow the scheme, in
        which case no revision can be attributed to it.
    """
    schema, parent = resolve_qualified_name(table, schema=schema)
    live = find_partition_for_value(conn, parent, key_value, schema=schema)
    if live is None:
        return None
    parsed = PartitionName.try_parse(live.name)
    if parsed is None or parsed.observation_id != key_value:
        raise PartitionNameError(
            f"{live.name!r} is attached to {parent} for {key_value} but "
            "does not follow the partition naming scheme, so it has no "
            "revision"
        )
    return parsed.revision


def revisions_of(
    conn: sa.Connection,
    table: TableRef,
    key_value: int,
    *,
    schema: str | None = None,
) -> dict[int, str]:
    """Every relation in the schema that names a revision of this slot.

    Attached, staged and retired relations all appear: they are
    indistinguishable by name, which is the point -- the name says which
    revision a table *is*, never whether it is live.

    Parameters
    ----------
    conn : sqlalchemy.Connection
        Connection to read with. Nothing is committed.
    table : str or sqlalchemy.Table or object with ``__table__``
        The partitioned parent.
    key_value : int
        The LIST value.
    schema : str, optional
        Schema to look in. Defaults to the parent's, else ``public``.

    Returns
    -------
    dict of int to str
        Revision number to relation name.
    """
    schema, parent = resolve_qualified_name(table, schema=schema)
    prefix = _like_prefix(f"{parent}_obs_{int(key_value)}")
    rows = conn.execute(
        sa.text(
            "SELECT c.relname FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = :schema AND c.relkind IN ('r', 'p') "
            "  AND c.relname LIKE :prefix ESCAPE '\\'"
        ),
        {"schema": schema, "prefix": prefix + "%"},
    ).scalars()

    found: dict[int, str] = {}
    for relname in rows:
        parsed = PartitionName.try_parse(relname)
        if (
            parsed is not None
            and parsed.base == parent
            and parsed.observation_id == key_value
        ):
            found[parsed.revision] = relname
    return found


def next_revision(
    conn: sa.Connection,
    table: TableRef,
    key_value: int,
    *,
    schema: str | None = None,
) -> int:
    """The revision number a new staging table for this slot should take.

    One past the highest that exists, counting retired and staged
    relations as well as the live one, so a replacement run years after
    the last one picks a number nothing else has used. A slot whose
    partitions have all been dropped will reuse a number -- consult the
    registry as well when that matters.

    Parameters
    ----------
    conn : sqlalchemy.Connection
        Connection to read with. Nothing is committed.
    table : str or sqlalchemy.Table or object with ``__table__``
        The partitioned parent.
    key_value : int
        The LIST value.
    schema : str, optional
        Schema to look in. Defaults to the parent's, else ``public``.

    Returns
    -------
    int
        ``0`` for an unprovisioned slot, so the first partition gets the
        unversioned name a DBA would have written by hand.
    """
    existing = revisions_of(conn, table, key_value, schema=schema)
    return max(existing) + 1 if existing else 0


def assert_paired_revisions(
    conn: sa.Connection,
    tables: Iterable[TableRef],
    key_value: int,
    *,
    schema: str | None = None,
) -> int | None:
    """Require several parents to be live at the same revision.

    Tables swapped together must stay together. ``dataset`` live at
    revision 4 while ``datasethierarchy`` is still at 3 means a swap was
    not atomic, and no ordinary operation can produce it.

    Parameters
    ----------
    conn : sqlalchemy.Connection
        Connection to read with. Nothing is committed.
    tables : iterable
        The parents that are swapped as one unit.
    key_value : int
        The LIST value they share.
    schema : str, optional
        Schema to look in. Defaults to each table's own, else
        ``public``.

    Returns
    -------
    int or None
        The revision they agree on, or ``None`` when none of them has
        the slot provisioned.

    Raises
    ------
    RevisionSkewError
        If they disagree, naming each table and its revision.

    Notes
    -----
    A parent with no relation at all for the key is skipped rather than
    counted as disagreeing: nothing was ever provisioned there, which
    is ordinary. A parent that *has* relations for the key but none
    attached is a different matter -- something was detached and not
    put back -- and does count as skew.
    """
    seen: dict[str, int | None] = {}
    for table in tables:
        _, name = resolve_qualified_name(table, schema=schema)
        live = live_revision(conn, table, key_value, schema=schema)
        if live is None and not revisions_of(
            conn, table, key_value, schema=schema
        ):
            # No relation of any revision exists for this key, so this
            # parent was never provisioned here. That is an ordinary
            # state -- a table with no rows for the observation -- not
            # the residue of a half-finished swap.
            continue
        seen[name] = live

    distinct = set(seen.values())
    if len(distinct) > 1:
        detail = ", ".join(
            f"{name} at {'nothing' if rev is None else rev}"
            for name, rev in sorted(seen.items())
        )
        raise RevisionSkewError(
            f"tables are live at different revisions for {key_value} "
            f"({detail}), which means a swap did not complete atomically"
        )
    return distinct.pop() if distinct else None


def _has_rows(conn: sa.Connection, schema: str, relation: str) -> bool:
    """Whether a relation holds at least one row.

    An existence probe rather than a count: it stops at the first tuple,
    so it stays cheap on a partition holding millions.
    """
    quote = conn.dialect.identifier_preparer.quote
    return bool(
        conn.execute(
            sa.text(
                "SELECT EXISTS (SELECT 1 FROM "
                f"{quote(schema)}.{quote(relation)} LIMIT 1)"
            )
        ).scalar()
    )


def _like_prefix(value: str) -> str:
    """Escape a literal string for use as a ``LIKE`` prefix."""
    for char in ("\\", "%", "_"):
        value = value.replace(char, "\\" + char)
    return value
