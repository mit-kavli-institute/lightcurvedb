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
