"""Promoting staged partitions as one atomic unit.

A replacement ends in four statements: detach the old partitions,
attach the new ones. Doing that for several parents inside a single
transaction is what makes an observation's data change all at once,
with no instant in which one table has been replaced and another has
not.

Everything expensive happens before the transaction. By the time
:func:`swap` runs, the incoming tables are loaded, indexed, analysed and
carrying every foreign key that can be pre-validated, so the work under
``ACCESS EXCLUSIVE`` is catalog manipulation and one unavoidable
foreign-key validation. What is left is measured in hundreds of
milliseconds on a 300k-row pair, and it is O(1) in row count except for
that validation.

Rolling back
------------
Before ``COMMIT`` the undo is ``ROLLBACK``, and that is the entire
reason this is one transaction rather than a sequence of concurrent
operations. After ``COMMIT``, :func:`rollback_swap` performs the mirror
swap, which works for exactly as long as the retired relations still
exist -- which is the coexistence window the whole design is for. After
:func:`drop_retired` there is no undo but a restore from backup, which
is why dropping is gated on explicit sign-off.

Transaction contract
--------------------
Every function takes a :class:`sqlalchemy.Connection` and **never
commits**. That is not a convenience here, it is the guarantee: the
caller's transaction boundary is the atomicity boundary. Anything else
that must be recorded as part of the same swap -- a registry row, an
audit note -- belongs in the caller's transaction, before its commit.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Iterable, Sequence

import sqlalchemy as sa

from lightcurvedb.core.partitions import ddl
from lightcurvedb.core.partitions.catalog import (
    AttachabilityReport,
    TableRef,
    check_attachable,
    find_partition_for_value,
    foreign_key_definitions,
    has_bound_check,
    relation_kind,
    require_list_partitioned,
    resolve_qualified_name,
)
from lightcurvedb.core.partitions.errors import (
    NotAttachableError,
    PartitionError,
    SwapRaceError,
)
from lightcurvedb.core.partitions.state import assert_paired_revisions

#: SQLSTATE PostgreSQL raises when ``lock_timeout`` expires.
LOCK_NOT_AVAILABLE = "55P03"


@dataclasses.dataclass(frozen=True, slots=True)
class SwapPair:
    """One parent's part of a swap.

    Attributes
    ----------
    parent : str
        The partitioned table.
    incoming : str
        The staged relation to attach.
    retiring : str or None
        The relation currently attached for the key, or ``None`` when
        the slot is empty and this is a first provisioning.
    retiring_schema : str or None
        Where ``retiring`` actually lives, read from the catalog rather
        than assumed, so a partition outside its parent's schema is
        detached where it is. ``None`` exactly when ``retiring`` is.
    """

    parent: str
    incoming: str
    retiring: str | None
    retiring_schema: str | None


@dataclasses.dataclass(frozen=True, slots=True)
class SwapPlan:
    """A resolved, ordered swap, ready to execute.

    Attributes
    ----------
    key_value : int
        The LIST value being replaced across every parent.
    schema : str
        Schema the partitioned parents live in.
    partition_schema : str
        Schema the *incoming* relations live in. Defaults to the
        parents' schema. Each retiring relation carries its own, which
        may differ while a deployment is moving its partitions.
    pairs : tuple of SwapPair
        Ordered so that a parent comes before any parent that
        references it. Attaches run in this order and detaches in
        reverse, which is what keeps every intermediate state legal.
    referenced : tuple of str
        Unpartitioned tables the parents' foreign keys point at. They
        are locked too, because attaching touches them.
    lock_timeout, statement_timeout : str or None
        Bounds for waiting and for holding.
    """

    key_value: int
    schema: str
    partition_schema: str
    pairs: tuple[SwapPair, ...]
    referenced: tuple[str, ...]
    lock_timeout: str | None
    statement_timeout: str | None

    @property
    def parents(self) -> tuple[str, ...]:
        """The partitioned tables involved, in attach order."""
        return tuple(pair.parent for pair in self.pairs)


@dataclasses.dataclass(frozen=True, slots=True)
class SwapStep:
    """What happened to one parent.

    Attributes
    ----------
    parent : str
        The partitioned table.
    promoted : str
        The relation now attached.
    retired : str or None
        The relation detached to make room, still on disk.
    retired_schema : str or None
        Where ``retired`` was, and still is. ``None`` exactly when
        ``retired`` is.
    """

    parent: str
    promoted: str
    retired: str | None
    retired_schema: str | None


@dataclasses.dataclass(frozen=True, slots=True)
class SwapResult:
    """What a swap did, and what a rollback would need.

    Attributes
    ----------
    key_value : int
        The LIST value replaced.
    schema : str
        Schema the partitioned parents live in.
    partition_schema : str
        Schema the promoted relations live in. Each retired relation
        carries its own on its :class:`SwapStep`.
    steps : tuple of SwapStep
        One per parent, in the order they were attached.
    """

    key_value: int
    schema: str
    partition_schema: str
    steps: tuple[SwapStep, ...]

    @property
    def promoted(self) -> tuple[str, ...]:
        """The relations now live."""
        return tuple(step.promoted for step in self.steps)

    @property
    def retired(self) -> tuple[str, ...]:
        """The relations detached, in the order they were attached."""
        return tuple(
            step.retired for step in self.steps if step.retired is not None
        )


@dataclasses.dataclass(frozen=True, slots=True)
class SwapPreflightReport:
    """What a swap would cost and whether it would work.

    Attributes
    ----------
    plan : SwapPlan
        The plan this describes.
    attachability : tuple of AttachabilityReport
        One per incoming relation, in plan order.
    orphans : tuple of str
        Incoming rows whose foreign keys point at rows no incoming
        relation supplies. PostgreSQL would catch these when it
        validates the cloned key, but only after the locks are held.
    holders : tuple of int
        Backend PIDs already holding a lock on a parent. Our ``ACCESS
        EXCLUSIVE`` request queues behind them -- and every reader
        arriving afterwards queues behind us.
    unprotected_retirements : tuple of str
        Partitions about to be retired that carry no validated bound
        CHECK. The swap itself does not care; a later rollback would
        have to re-read all their rows under lock.
        :func:`prepare_retirement` fixes this beforehand.
    """

    plan: SwapPlan
    attachability: tuple[AttachabilityReport, ...]
    orphans: tuple[str, ...]
    holders: tuple[int, ...]
    unprotected_retirements: tuple[str, ...]

    @property
    def blocking(self) -> tuple[str, ...]:
        """Findings that would make the swap fail."""
        found: list[str] = []
        for report in self.attachability:
            found.extend(
                f"{report.candidate}: {finding}" for finding in report.blocking
            )
        found.extend(self.orphans)
        return tuple(found)

    @property
    def expensive(self) -> tuple[str, ...]:
        """Findings that would make the swap slow while holding locks."""
        found: list[str] = []
        for report in self.attachability:
            found.extend(
                f"{report.candidate}: {finding}"
                for finding in report.expensive
            )
        found.extend(
            f"{relation}: no validated bound CHECK, so rolling this swap "
            "back would rescan it under lock. Run prepare_retirement "
            "first."
            for relation in self.unprotected_retirements
        )
        return tuple(found)

    @property
    def ok(self) -> bool:
        """Whether the swap would be a pure catalog change."""
        return not self.blocking and not self.expensive

    def raise_for_status(self, *, allow_expensive: bool = False) -> None:
        """Raise unless the swap is safe to run.

        Parameters
        ----------
        allow_expensive : bool, default False
            Tolerate findings that only make the swap slow. Some are
            unavoidable -- a partition referencing another partitioned
            table cannot pre-validate its foreign keys -- so a
            production promotion usually passes ``True`` after reading
            :attr:`expensive` rather than ignoring it.

        Raises
        ------
        NotAttachableError
            Listing every finding that applies.
        """
        found = list(self.blocking)
        if not allow_expensive:
            found.extend(self.expensive)
        if found:
            raise NotAttachableError(
                f"swap of {self.key_summary} would not be clean: "
                + "; ".join(found)
            )

    @property
    def key_summary(self) -> str:
        """A short description of what this swap covers."""
        return f"{', '.join(self.plan.parents)} at {self.plan.key_value}"


def plan_swap(
    conn: sa.Connection,
    pairs: Iterable[tuple[TableRef, str]],
    key_value: int,
    *,
    schema: str | None = None,
    partition_schema: str | None = None,
    lock_timeout: str | None = "5s",
    statement_timeout: str | None = "120s",
) -> SwapPlan:
    """Resolve what a swap would do, without touching anything.

    Reads which relation currently holds the key for each parent, works
    out the order the parents have to be handled in, and collects the
    tables that need locking alongside them.

    Parameters
    ----------
    conn : sqlalchemy.Connection
        Connection to read with. Nothing is committed.
    pairs : iterable of (table, str)
        Each parent and the staged relation to promote for it.
    key_value : int
        The LIST value being replaced.
    schema : str, optional
        Schema of the partitioned parents. Defaults to the first
        parent's, else ``public``.
    partition_schema : str, optional
        Schema the *incoming* relations live in. Defaults to the
        parents'. The retiring side is not covered by this: each one is
        found in the catalog and detached wherever it actually is.
    lock_timeout, statement_timeout : str or None
        Recorded on the plan and applied by :func:`swap`.

    Returns
    -------
    SwapPlan

    Raises
    ------
    ValueError
        If no pairs are given, if a parent appears twice, or if they do
        not all share a schema.
    PartitionError
        If the parents' foreign keys form a cycle, leaving no order in
        which every intermediate state is legal, or if a partition
        being retired holds more LIST values than the one being
        replaced -- attaching its successor would silently strip the
        others of their partition.
    RelationNotFoundError
        If a parent does not exist.

    Notes
    -----
    Order is derived, not assumed: a parent that references another is
    detached first and attached last. That way no visible instant has a
    row referencing a partition that is not there, and the referencing
    side's validation runs against keys that are already in place.
    """
    resolved: list[tuple[str, str, str]] = []
    for table, incoming in pairs:
        table_schema, parent = resolve_qualified_name(table, schema=schema)
        resolved.append((table_schema, parent, incoming))
    if not resolved:
        raise ValueError("a swap needs at least one parent")

    schemas = {entry[0] for entry in resolved}
    if len(schemas) > 1:
        raise ValueError(f"all parents must share one schema, got {schemas}")
    schema = schemas.pop()
    partition_schema = partition_schema or schema

    unordered: dict[str, SwapPair] = {}
    for _, parent, incoming in resolved:
        if parent in unordered:
            raise ValueError(
                f"{parent} appears twice in the same swap, promoting "
                f"{unordered[parent].incoming} and {incoming}. Only one "
                "relation can hold a bound, so the plan is ambiguous."
            )
        require_list_partitioned(conn, parent, schema=schema)
        live = find_partition_for_value(conn, parent, key_value, schema=schema)
        if live is not None and live.list_values != (key_value,):
            raise PartitionError(
                f"{live.name} is attached FOR VALUES IN "
                f"{list(live.list_values)}, so replacing it for "
                f"{key_value} alone would strip the other values of "
                "their partition. Split the bound first."
            )
        unordered[parent] = SwapPair(
            parent=parent,
            incoming=incoming,
            retiring=None if live is None else live.name,
            retiring_schema=None if live is None else live.schema,
        )

    ordered = _order_by_references(conn, unordered, schema)
    referenced = ddl.referenced_tables(conn, ordered.keys(), schema=schema)
    return SwapPlan(
        key_value=key_value,
        schema=schema,
        partition_schema=partition_schema,
        pairs=tuple(ordered.values()),
        referenced=referenced,
        lock_timeout=lock_timeout,
        statement_timeout=statement_timeout,
    )


def preflight(
    conn: sa.Connection,
    plan: SwapPlan,
    *,
    check_orphans: bool = True,
) -> SwapPreflightReport:
    """Find out what the swap would cost, before any lock is taken.

    Parameters
    ----------
    conn : sqlalchemy.Connection
        Connection to read with. Nothing is committed.
    plan : SwapPlan
        The plan to examine.
    check_orphans : bool, default True
        Look for incoming rows that reference rows no incoming relation
        supplies. This reads the staged tables and so costs a scan;
        skipping it does not make the swap less safe, only later to
        fail.

    Returns
    -------
    SwapPreflightReport

    Raises
    ------
    RevisionSkewError
        If the parents are not currently live at a single revision.
        That is a pre-existing inconsistency, not something this swap
        would cause, so it stops the swap rather than being reported.
    """
    assert_paired_revisions(
        conn,
        plan.parents,
        plan.key_value,
        schema=plan.schema,
        partition_schema=plan.partition_schema,
    )
    reports = tuple(
        check_attachable(
            conn,
            pair.parent,
            pair.incoming,
            plan.key_value,
            schema=plan.schema,
            partition_schema=plan.partition_schema,
        )
        for pair in plan.pairs
    )
    orphans = _orphan_findings(conn, plan) if check_orphans else ()
    return SwapPreflightReport(
        plan=plan,
        attachability=reports,
        orphans=orphans,
        holders=blocking_pids(conn, plan.parents, schema=plan.schema),
        unprotected_retirements=_unprotected_retirements(conn, plan),
    )


def swap(conn: sa.Connection, plan: SwapPlan) -> SwapResult:
    """Execute the plan: detach the old partitions, attach the new.

    Parameters
    ----------
    conn : sqlalchemy.Connection
        Connection to emit on, inside the caller's transaction.
        **Nothing is committed** -- until the caller commits, the whole
        swap is undone by rolling back.
    plan : SwapPlan
        A plan from :func:`plan_swap`, ideally after :func:`preflight`.

    Returns
    -------
    SwapResult
        What was promoted and what was retired, enough for
        :func:`rollback_swap` to reverse it later.

    Raises
    ------
    SwapRaceError
        If, once the locks are held, a parent's live partition is not
        the one the plan expected. Somebody else promoted something in
        between, and their work must not be discarded blindly.

    Notes
    -----
    The sequence is: bound both timeouts, take every lock up front in
    one canonical order, re-check what is live, detach
    referencing-side-first, attach referenced-side-first. Nothing here
    reads a row, so the critical section is O(1) in table size apart
    from foreign-key validation on a referencing side that could not
    pre-create its keys.

    In particular this does **not** add a bound CHECK to the relations
    it retires, though a retired relation wants one: without it a
    rollback re-reads every row. Validating a CHECK means scanning, and
    scanning here would happen with every parent still locked --
    turning a bounded swap into an outage proportional to the data.
    Partitions created by
    :func:`~lightcurvedb.core.partitions.bootstrap.ensure_partition` and
    staging tables carry the constraint from birth, so they need
    nothing; for a partition created by hand, run
    :func:`prepare_retirement` before the swap transaction.
    :func:`preflight` says when that applies.

    No integrity gate is needed inside the transaction. PostgreSQL
    refuses to detach a partition whose rows another table's foreign key
    still references, so a swap that would strand data fails loudly on
    its own rather than being caught by a check that has to be
    remembered.

    The retiring partitions are not named in the up-front lock, and do
    not need to be: ``LOCK TABLE`` without ``ONLY`` recurses to every
    descendant, and ``pg_inherits`` does not care about schemas, so
    locking the parents already takes ``ACCESS EXCLUSIVE`` on every
    attached partition wherever it lives. The incoming relations are not
    descendants of anything, so they take their lock at ``ATTACH``.
    """
    ddl.set_local_timeouts(
        conn,
        lock_timeout=plan.lock_timeout,
        statement_timeout=plan.statement_timeout,
    )
    ddl.lock_tables(
        conn, sorted(plan.parents), "ACCESS EXCLUSIVE", schema=plan.schema
    )
    if plan.referenced:
        ddl.lock_tables(
            conn,
            sorted(plan.referenced),
            "SHARE ROW EXCLUSIVE",
            schema=plan.schema,
        )

    for pair in plan.pairs:
        live = find_partition_for_value(
            conn, pair.parent, plan.key_value, schema=plan.schema
        )
        # Compare where as well as what: the same relation name in two
        # schemas is two different relations.
        current = None if live is None else (live.schema, live.name)
        expected = (
            None
            if pair.retiring is None
            else (str(pair.retiring_schema), pair.retiring)
        )
        if current != expected:
            raise SwapRaceError(
                f"{pair.parent} now has "
                f"{_render(current)} attached for {plan.key_value}, "
                f"but the plan expected {_render(expected)}. "
                "Re-plan against the current state."
            )

    for pair in reversed(plan.pairs):
        if pair.retiring is not None:
            ddl.detach_partition(
                conn,
                pair.parent,
                pair.retiring,
                schema=plan.schema,
                partition_schema=pair.retiring_schema,
            )

    for pair in plan.pairs:
        ddl.attach_partition(
            conn,
            pair.parent,
            pair.incoming,
            plan.key_value,
            schema=plan.schema,
            partition_schema=plan.partition_schema,
            preflight=False,
        )

    return SwapResult(
        key_value=plan.key_value,
        schema=plan.schema,
        partition_schema=plan.partition_schema,
        steps=tuple(
            SwapStep(
                parent=pair.parent,
                promoted=pair.incoming,
                retired=pair.retiring,
                retired_schema=pair.retiring_schema,
            )
            for pair in plan.pairs
        ),
    )


def prepare_retirement(conn: sa.Connection, plan: SwapPlan) -> tuple[str, ...]:
    """Give each partition about to be retired its bound CHECK.

    Run this **before** the swap transaction, not inside it. Validating
    a CHECK means reading every row, and the swap holds ``ACCESS
    EXCLUSIVE`` on every parent -- so doing it there would block every
    reader of every partition for as long as the scan takes. Done here
    the scan locks one partition, briefly, while the rest of the table
    carries on.

    A partition that already carries the constraint is skipped, so this
    is a no-op for anything
    :func:`~lightcurvedb.core.partitions.bootstrap.ensure_partition`
    created. It exists for partitions created by hand, which have only
    the implicit bound PostgreSQL derives from ``FOR VALUES IN`` -- and
    that is not kept when the partition is detached.

    Parameters
    ----------
    conn : sqlalchemy.Connection
        Connection to emit on. Nothing is committed.
    plan : SwapPlan
        The plan whose retiring partitions to prepare.

    Returns
    -------
    tuple of str
        Names of the constraints added, empty when there was nothing to
        do.

    Notes
    -----
    Why a retired relation needs a constraint it no longer has to
    satisfy: a plain ``DETACH`` leaves no CHECK behind, so re-attaching
    the relation later -- which is exactly what a rollback does -- makes
    PostgreSQL prove the bound by reading the whole table. Measured at
    300k rows that is the difference between roughly 10 ms and 1 ms,
    and it scales with the data while everything else in the swap does
    not.
    """
    added: list[str] = []
    for pair in plan.pairs:
        if pair.retiring is None:
            continue
        strategy = require_list_partitioned(
            conn, pair.parent, schema=plan.schema
        )
        name = ddl.add_bound_check(
            conn,
            pair.retiring,
            strategy.key_column,
            plan.key_value,
            schema=pair.retiring_schema,
        )
        if name is not None:
            added.append(name)
    return tuple(added)


def rollback_swap(
    conn: sa.Connection,
    result: SwapResult,
    *,
    lock_timeout: str | None = "5s",
    statement_timeout: str | None = "120s",
) -> SwapResult:
    """Put back what a committed swap replaced.

    The mirror of :func:`swap`: the retired relations go back in and the
    promoted ones come out. Valid only while the retired relations still
    exist, which is until :func:`drop_retired` runs.

    Parameters
    ----------
    conn : sqlalchemy.Connection
        Connection to emit on, inside the caller's transaction. Nothing
        is committed.
    result : SwapResult
        What :func:`swap` returned.
    lock_timeout, statement_timeout : str or None
        Bounds for this transaction, as for :func:`plan_swap`.

    Returns
    -------
    SwapResult
        Describing the reversal, so it can itself be reversed.

    Raises
    ------
    PartitionError
        If any step has nothing to roll back to, because the promotion
        filled a previously empty slot. Detach the promoted relation
        instead -- there is no earlier revision to restore.
    RelationNotFoundError
        If a retired relation has since been dropped.
    """
    missing = [step.parent for step in result.steps if step.retired is None]
    if missing:
        raise PartitionError(
            "cannot roll back "
            + ", ".join(missing)
            + f" at {result.key_value}: the promotion filled an empty slot, "
            "so there is no earlier revision to restore"
        )

    # The two sides cross over, so the schemas do too: what was retired
    # becomes incoming, and what was promoted becomes retiring.
    retired_schemas = {str(step.retired_schema) for step in result.steps}
    if len(retired_schemas) > 1:
        raise PartitionError(
            "the retired relations are spread across "
            + ", ".join(sorted(retired_schemas))
            + ", so one rollback plan cannot address them all"
        )

    pairs = tuple(
        SwapPair(
            parent=step.parent,
            incoming=str(step.retired),
            retiring=step.promoted,
            retiring_schema=result.partition_schema,
        )
        for step in result.steps
    )
    plan = SwapPlan(
        key_value=result.key_value,
        schema=result.schema,
        partition_schema=retired_schemas.pop(),
        pairs=pairs,
        referenced=ddl.referenced_tables(
            conn, [pair.parent for pair in pairs], schema=result.schema
        ),
        lock_timeout=lock_timeout,
        statement_timeout=statement_timeout,
    )
    return swap(conn, plan)


def drop_retired(
    conn: sa.Connection,
    relations: Iterable[str],
    *,
    schema: str | None = None,
    dry_run: bool = True,
) -> tuple[str, ...]:
    """Drop detached relations for good.

    Parameters
    ----------
    conn : sqlalchemy.Connection
        Connection to emit on. Nothing is committed.
    relations : iterable of str
        The relations to drop. Pass only relations that have been signed
        off -- this function has no way to know, by design: the record
        of who accepted what lives in the registry, and this package
        does not read it.
    schema : str, optional
        Schema to look in. Defaults to ``public``.
    dry_run : bool, default True
        Report what would be dropped and drop nothing. The default is
        deliberate: this is the one step in a replacement that cannot be
        undone.

    Returns
    -------
    tuple of str
        Relations dropped, or that would be. Names that do not exist are
        skipped silently, so a repeated run is a no-op.

    Raises
    ------
    PartitionError
        If a named relation is still attached to a parent. That is live
        data, and no sign-off makes dropping it part of a retirement.
    """
    schema = schema or "public"
    targets: list[str] = []
    for relation in relations:
        if relation_kind(conn, relation, schema=schema) is None:
            continue
        if _is_attached(conn, schema, relation):
            raise PartitionError(
                f"{schema}.{relation} is still attached to a parent and "
                "holds live data, so it is not a retired relation"
            )
        targets.append(relation)

    if not dry_run:
        for relation in targets:
            ddl.drop_relation(conn, relation, schema=schema)
    return tuple(targets)


def blocking_pids(
    conn: sa.Connection,
    tables: Iterable[TableRef],
    *,
    schema: str | None = None,
) -> tuple[int, ...]:
    """Backends already holding a lock on any of ``tables``.

    Worth checking before asking for ``ACCESS EXCLUSIVE``. The request
    waits behind every lock already granted, and once it is waiting,
    every new reader waits behind *it* -- so a swap fired while a long
    analytical query is running stalls the whole table for that query's
    remaining lifetime, not just for the swap's.

    Parameters
    ----------
    conn : sqlalchemy.Connection
        Connection to read with. Nothing is committed.
    tables : iterable
        The relations to check.
    schema : str, optional
        Schema for tables that do not carry one. Defaults to ``public``.

    Returns
    -------
    tuple of int
        Distinct PIDs, sorted, excluding this backend.
    """
    names = [
        resolve_qualified_name(table, schema=schema)[1] for table in tables
    ]
    if not names:
        return ()
    rows = conn.execute(
        sa.text(
            "SELECT DISTINCT l.pid FROM pg_locks l "
            "JOIN pg_class c ON c.oid = l.relation "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = :schema AND c.relname = ANY(:names) "
            "  AND l.granted AND l.pid <> pg_backend_pid() "
            "ORDER BY l.pid"
        ),
        {"schema": schema or "public", "names": names},
    ).scalars()
    return tuple(rows)


def is_lock_not_available(error: BaseException) -> bool:
    """Whether an exception is PostgreSQL's ``lock_timeout`` firing.

    ``lock_timeout`` bounds the wait, nothing more: when it expires the
    statement fails and the transaction is dead. Retrying is the
    caller's job, because only the caller knows whether its transaction
    can be replayed -- back off with jitter, and log
    :func:`blocking_pids` each time so a persistent blocker is visible
    rather than merely slow.

    Parameters
    ----------
    error : BaseException
        Usually a :class:`sqlalchemy.exc.OperationalError`.

    Returns
    -------
    bool
        True if the underlying SQLSTATE is ``55P03``.
    """
    original = getattr(error, "orig", error)
    for attribute in ("sqlstate", "pgcode"):
        if getattr(original, attribute, None) == LOCK_NOT_AVAILABLE:
            return True
    return False


def _render(target: tuple[str, str] | None) -> str:
    """``schema.relation``, or ``"nothing"`` for an empty slot."""
    return "nothing" if target is None else f"{target[0]}.{target[1]}"


def _unprotected_retirements(
    conn: sa.Connection, plan: SwapPlan
) -> tuple[str, ...]:
    """Retiring partitions with no validated bound CHECK of their own."""
    found: list[str] = []
    for pair in plan.pairs:
        if pair.retiring is None:
            continue
        strategy = require_list_partitioned(
            conn, pair.parent, schema=plan.schema
        )
        if not has_bound_check(
            conn,
            pair.retiring,
            strategy.key_column,
            plan.key_value,
            schema=pair.retiring_schema,
        ):
            found.append(pair.retiring)
    return tuple(found)


def _order_by_references(
    conn: sa.Connection, pairs: dict[str, SwapPair], schema: str
) -> dict[str, SwapPair]:
    """Sort parents so a referenced table comes before its referencer."""
    edges: dict[str, set[str]] = {name: set() for name in pairs}
    for name in pairs:
        for spec in foreign_key_definitions(conn, name, schema=schema):
            if (
                spec.referenced_table in pairs
                and spec.referenced_table != name
            ):
                edges[name].add(spec.referenced_table)

    ordered: dict[str, SwapPair] = {}
    remaining = dict(edges)
    while remaining:
        ready = sorted(
            name
            for name, deps in remaining.items()
            if not deps - ordered.keys()
        )
        if not ready:
            raise PartitionError(
                "foreign keys between "
                + ", ".join(sorted(remaining))
                + " form a cycle, so no detach order leaves every "
                "intermediate state valid"
            )
        for name in ready:
            ordered[name] = pairs[name]
            del remaining[name]
    return ordered


def _orphan_findings(conn: sa.Connection, plan: SwapPlan) -> tuple[str, ...]:
    """Incoming rows referencing keys no incoming relation supplies."""
    by_parent = {pair.parent: pair for pair in plan.pairs}
    findings: list[str] = []
    for pair in plan.pairs:
        for spec in foreign_key_definitions(
            conn, pair.parent, schema=plan.schema
        ):
            target = by_parent.get(spec.referenced_table)
            if target is None or target.parent == pair.parent:
                continue
            orphans = _count_orphans(
                conn,
                plan.partition_schema,
                pair.incoming,
                spec.columns,
                target.incoming,
                spec.referenced_columns,
            )
            if orphans:
                findings.append(
                    f"{pair.incoming}: {orphans} row(s) reference keys "
                    f"absent from {target.incoming} "
                    f"(via {spec.name})"
                )
    return tuple(findings)


def _count_orphans(
    conn: sa.Connection,
    schema: str,
    referencing: str,
    columns: Sequence[str],
    referenced: str,
    referenced_columns: Sequence[str],
) -> int:
    quote = conn.dialect.identifier_preparer.quote
    joins = " AND ".join(
        f"t.{quote(theirs)} = r.{quote(ours)}"
        for ours, theirs in zip(columns, referenced_columns)
    )
    # A MATCH SIMPLE foreign key is satisfied whenever any of its
    # columns is NULL, so those rows are not orphans however they look.
    not_null = " AND ".join(
        f"r.{quote(column)} IS NOT NULL" for column in columns
    )
    return int(
        conn.execute(
            sa.text(
                f"SELECT count(*) FROM {quote(schema)}.{quote(referencing)} r "
                f"WHERE {not_null} AND NOT EXISTS ("
                f"SELECT 1 FROM {quote(schema)}.{quote(referenced)} t "
                f"WHERE {joins})"
            )
        ).scalar()
        or 0
    )


def _is_attached(conn: sa.Connection, schema: str, relation: str) -> bool:
    return bool(
        conn.execute(
            sa.text(
                "SELECT c.relispartition FROM pg_class c "
                "JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = :schema AND c.relname = :relation"
            ),
            {"schema": schema, "relation": relation},
        ).scalar()
    )
