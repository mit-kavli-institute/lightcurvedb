"""Physical names for partitions and the objects attached to them.

A partition of a LIST-partitioned parent is named
``<base>_obs_<observation_id>_v<revision>``. Revision 0 is the legacy,
unversioned form ``<base>_obs_<observation_id>`` documented in the
CHANGELOG, so partitions created by hand before this module existed parse
without special casing and a freshly provisioned observation gets the
name a DBA would have written.

Names are immutable for the life of a relation. Promoting a revision never
renames anything -- it detaches one table and attaches another -- so which
revision is live is answered by ``pg_inherits``, never by the name. The
human-readable campaign label belongs in the provenance registry.

Everything here is pure: no database, no SQLAlchemy metadata. That is what
lets it be tested exhaustively with Hypothesis.

.. note::
   Unrelated to :func:`lightcurvedb.util.iter.eq_partitions`, which splits
   an in-memory iterable into equal chunks.
"""

from __future__ import annotations

import dataclasses
import re
from typing import ClassVar, Final

from lightcurvedb.core.partitions.errors import (
    PartitionNameError,
    PartitionNameTooLongError,
)

#: PostgreSQL's ``NAMEDATALEN - 1``. Longer identifiers are silently
#: truncated, so every name this module produces is checked against it.
MAX_IDENTIFIER_BYTES: Final[int] = 63

#: Suffixes of the objects created alongside a partition. Which of these
#: a given parent actually needs is the DDL layer's concern; naming only
#: guarantees that every one of them fits in an identifier.
OBJECT_SUFFIXES: Final[tuple[str, ...]] = (
    "pkey",
    "target_idx",
    "src_idx",
    "child_idx",
    "partcheck",
)

_IDENTIFIER: Final = re.compile(r"^[a-z_][a-z0-9_]*$")
_SUFFIX: Final = re.compile(r"^[a-z][a-z0-9_]*$")

# Canonical form only: no leading zeros, and revision 0 is spelled by
# omitting the ``_v`` segment. Rejecting ``_v0`` and ``_obs_05`` means every
# accepted string re-renders to itself, so parse and render are inverses.
_PARTITION_NAME: Final = re.compile(
    r"^(?P<base>[a-z_][a-z0-9_]*?)"
    r"_obs_(?P<observation_id>0|[1-9][0-9]*)"
    r"(?:_v(?P<revision>[1-9][0-9]*))?$"
)


@dataclasses.dataclass(frozen=True, slots=True)
class PartitionName:
    """The physical name of one partition revision and its objects.

    Parameters
    ----------
    base : str
        The partitioned parent table, e.g. ``"dataset"``. Must be an
        unquoted lowercase PostgreSQL identifier.
    observation_id : int
        The LIST partition key value. Non-negative.
    revision : int, optional
        Monotonic per ``(base, observation_id)``. ``0`` is the legacy
        unversioned form and the default.

    Raises
    ------
    PartitionNameError
        If ``base`` is not a lowercase identifier, or either integer is
        negative.
    PartitionNameTooLongError
        If the table name or any documented derived object name would
        exceed :data:`MAX_IDENTIFIER_BYTES`.

    Examples
    --------
    >>> PartitionName("dataset", 5).table
    'dataset_obs_5'
    >>> PartitionName("dataset", 5, revision=3).table
    'dataset_obs_5_v3'
    >>> PartitionName("datasethierarchy", 5, 3).derived("child_idx")
    'datasethierarchy_obs_5_v3_child_idx'
    >>> PartitionName.parse("dataset_obs_5_v3")
    PartitionName(base='dataset', observation_id=5, revision=3)
    """

    base: str
    observation_id: int
    revision: int = 0

    KEY_INFIX: ClassVar[str] = "obs"
    REVISION_PREFIX: ClassVar[str] = "v"

    def __post_init__(self) -> None:
        if not _IDENTIFIER.match(self.base):
            raise PartitionNameError(
                f"base {self.base!r} is not a lowercase PostgreSQL identifier"
            )
        if self.observation_id < 0:
            raise PartitionNameError(
                "observation_id must be non-negative, "
                f"got {self.observation_id}"
            )
        if self.revision < 0:
            raise PartitionNameError(
                f"revision must be non-negative, got {self.revision}"
            )
        # Validate the longest documented object name up front, so a
        # PartitionName that constructs can name every object it owns.
        longest = max(OBJECT_SUFFIXES, key=len)
        self.derived(longest)

    @property
    def table(self) -> str:
        """The partition's relation name."""
        name = f"{self.base}_{self.KEY_INFIX}_{self.observation_id}"
        if self.revision:
            name = f"{name}_{self.REVISION_PREFIX}{self.revision}"
        return name

    def derived(self, suffix: str) -> str:
        """Name of an object attached to this partition.

        Parameters
        ----------
        suffix : str
            A lowercase identifier fragment such as ``"pkey"`` or
            ``"partcheck"``; see :data:`OBJECT_SUFFIXES` for the documented
            set.

        Returns
        -------
        str
            ``<table>_<suffix>``.

        Raises
        ------
        PartitionNameError
            If ``suffix`` is not a lowercase identifier fragment.
        PartitionNameTooLongError
            If the result would exceed :data:`MAX_IDENTIFIER_BYTES`.
        """
        if not _SUFFIX.match(suffix):
            raise PartitionNameError(
                f"suffix {suffix!r} is not a lowercase identifier fragment"
            )
        name = f"{self.table}_{suffix}"
        size = len(name.encode())
        if size > MAX_IDENTIFIER_BYTES:
            raise PartitionNameTooLongError(
                f"{name!r} is {size} bytes, but PostgreSQL identifiers are "
                f"limited to {MAX_IDENTIFIER_BYTES}"
            )
        return name

    @classmethod
    def parse(cls, name: str) -> PartitionName:
        """Recover a :class:`PartitionName` from a relation name.

        Accepts only the canonical form produced by :attr:`table`, so
        ``parse(pn.table) == pn`` and ``parse(s).table == s`` for every
        accepted ``s``.

        Raises
        ------
        PartitionNameError
            If ``name`` is not a canonical partition name.
        """
        match = _PARTITION_NAME.match(name)
        if match is None:
            raise PartitionNameError(
                f"{name!r} is not a partition name of the form "
                f"<base>_{cls.KEY_INFIX}_<observation_id>"
                f"[_{cls.REVISION_PREFIX}<revision>]"
            )
        revision = match["revision"]
        return cls(
            base=match["base"],
            observation_id=int(match["observation_id"]),
            revision=int(revision) if revision is not None else 0,
        )

    @classmethod
    def try_parse(cls, name: str) -> PartitionName | None:
        """Like :meth:`parse`, returning ``None`` for a non-partition name.

        For sweeping a catalog listing where most relations are ordinary
        tables.
        """
        try:
            return cls.parse(name)
        except PartitionNameError:
            return None

    def __str__(self) -> str:
        return self.table
