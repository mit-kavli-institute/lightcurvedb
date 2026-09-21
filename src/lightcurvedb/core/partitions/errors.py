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
