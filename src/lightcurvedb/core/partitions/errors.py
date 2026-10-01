"""Exceptions raised by partition management.

Every error in this package derives from :class:`PartitionError`, so a
caller can catch the whole family with one clause. Errors about a
relation *name* additionally derive from :class:`ValueError`, since a
malformed name is an invalid value in the ordinary Python sense.
"""


class PartitionError(Exception):
    """Base class for every partition-management error."""


class PartitionNameError(PartitionError, ValueError):
    """A relation name is not a valid or parseable partition name."""


class PartitionNameTooLongError(PartitionNameError):
    """A derived identifier would exceed PostgreSQL's 63-byte limit.

    PostgreSQL silently truncates longer identifiers to ``NAMEDATALEN - 1``
    bytes, which turns two distinct names into one and surfaces later as a
    confusing "relation already exists". Raising here keeps that failure at
    the point the name is formed.
    """


class RelationNotFoundError(PartitionError):
    """A named relation does not exist in the database."""


class NotPartitionedError(PartitionError):
    """The relation exists but is not a partitioned table."""


class UnsupportedPartitionStrategyError(PartitionError):
    """The table is partitioned in a way this package does not handle.

    Supported: LIST partitioning on a single plain column. RANGE, HASH,
    multi-column keys and expression keys are refused rather than
    guessed at.
    """


class NotAttachableError(PartitionError):
    """A candidate table cannot be attached as a partition cleanly.

    Raised by :meth:`AttachabilityReport.raise_for_status`; the message
    lists every finding, split into what would make ``ATTACH`` fail and
    what would make it scan or build while holding ``ACCESS EXCLUSIVE``.
    """


class StagingShapeMismatchError(PartitionError):
    """A staging table's columns do not match the parent's.

    ``CREATE TABLE IF NOT EXISTS`` accepts a table built from an older
    definition of the parent without complaint, so a stale staging table
    would otherwise be discovered at attach time, under
    ``ACCESS EXCLUSIVE``. Comparing the two column sets up front moves
    that failure to the cheapest possible moment.
    """


class AutocommitRequiredError(PartitionError):
    """An operation was attempted inside a transaction that forbids one.

    ``ALTER TABLE ... DETACH PARTITION ... CONCURRENTLY`` cannot run in a
    transaction block. SQLAlchemy opens one implicitly, so the connection
    must carry ``isolation_level="AUTOCOMMIT"``. Checking before the
    statement is issued keeps the caller's transaction unpoisoned.
    """


class IllegalStateTransitionError(PartitionError):
    """A revision was moved between two states with no legal edge.

    The lifecycle is a small directed graph, not a set of independent
    flags; refusing an unlisted edge catches bookkeeping mistakes -- such
    as accepting a revision that was never swapped in -- before they are
    written down.
    """


class SwapRaceError(PartitionError):
    """The live partition changed between planning and swapping.

    A plan names the relation it expects to retire. If another session
    promoted a different revision in the meantime, continuing would
    detach something the plan never inspected, so the swap aborts with
    its transaction intact.
    """


class RevisionSkewError(PartitionError):
    """Paired tables are live at different revisions for one key.

    ``dataset`` at revision 4 while ``datasethierarchy`` is still at 3
    means a swap was not atomic. Nothing in normal operation can produce
    it, so it is reported rather than repaired.
    """
