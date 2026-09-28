Partition Management
====================

``dataset``, ``datasethierarchy`` and ``target_specific_time`` are
PostgreSQL LIST-partitioned tables keyed on an observation id, so one
observation's data lives in one partition of each. The
:mod:`lightcurvedb.core.partitions` package works with those partitions
directly: naming them, reading what is attached, and predicting -- before
any lock is taken -- what attaching a new one would cost.

This page covers what exists today: the naming policy and read-only
introspection. Creating, attaching, detaching and swapping partitions
arrive in later releases; the design they follow is recorded in
``docs/PARTITION_REPLACEMENT_REQUIREMENTS.md``.

Concepts
--------

**One observation, one partition.** A LIST partition bound such as
``FOR VALUES IN (5)`` may be claimed by exactly one attached partition, so
for a given observation the *live* data is whichever relation is attached
for that value. Anything else holding that observation's data is a
standalone table -- staged and waiting, or retired and awaiting deletion.

**The catalog is the truth.** Which relation is attached, with what bound,
is read from ``pg_catalog`` every time. No registry is consulted, so the
answers here cannot drift from what PostgreSQL will actually do.

**Names are immutable.** A partition keeps the name it was born with. When
one revision replaces another the old table is detached and the new one
attached; nothing is renamed. Consequently the name tells you *which
revision a table is*, never *whether it is live* -- ask the catalog for
that.

**Everything is a read.** Every function on this page takes a
:class:`sqlalchemy.engine.Connection` and never commits. Get one from a
session with ``session.connection()`` or from an engine with
``engine.connect()``.

.. note::
   :func:`lightcurvedb.util.iter.eq_partitions` splits an in-memory
   iterable into equal chunks. It has nothing to do with PostgreSQL and
   nothing to do with this package; the shared word is a coincidence.

Naming partitions
-----------------

Partitions are named ``<base>_obs_<observation_id>_v<revision>``.
Revision ``0`` is the legacy unversioned form ``<base>_obs_<id>`` that a
DBA would write by hand, so existing partitions parse without special
casing.

.. code-block:: python

   from lightcurvedb.core.partitions import PartitionName

   name = PartitionName("dataset", 5)
   name.table                      # 'dataset_obs_5'

   name = PartitionName("dataset", 5, revision=3)
   name.table                      # 'dataset_obs_5_v3'
   name.derived("pkey")            # 'dataset_obs_5_v3_pkey'
   name.derived("partcheck")       # 'dataset_obs_5_v3_partcheck'

   PartitionName.parse("dataset_obs_5_v3")
   # PartitionName(base='dataset', observation_id=5, revision=3)

   PartitionName.try_parse("observation")   # None -- not a partition name

Parsing accepts only the canonical spelling: no leading zeros, and
revision 0 is written by omitting ``_v``. That makes ``parse`` and
``table`` exact inverses, so a name read from the catalog maps back to
the object that produced it.

Every derived name is checked against PostgreSQL's 63-byte identifier
limit when the :class:`~lightcurvedb.core.partitions.PartitionName` is
constructed. A name that constructs can therefore name every object it
owns; one that cannot raises
:class:`~lightcurvedb.core.partitions.PartitionNameTooLongError` at that
point rather than letting ``CREATE INDEX`` silently truncate later.

Reading what is attached
------------------------

.. code-block:: python

   from lightcurvedb.core.partitions import (
       default_partition_of,
       find_partition_for_value,
       list_table_partitions,
       partition_strategy,
   )

   conn = session.connection()

   strategy = partition_strategy(conn, "dataset")
   strategy.strategy        # 'list'
   strategy.key_column      # 'observation_id'

   for info in list_table_partitions(conn, "dataset"):
       print(info.name, info.bound, info.list_values, info.total_bytes)

   live = find_partition_for_value(conn, "dataset", 5)
   live.name if live else None    # 'dataset_obs_5', or None if unprovisioned

Tables may be passed as a name, a :class:`sqlalchemy.Table`, or a mapped
class such as ``DataSet``.

Two things about :class:`~lightcurvedb.core.partitions.PartitionInfo` are
easy to get wrong:

* ``total_bytes`` is the only size that includes TOAST. For tables whose
  rows are float arrays, TOAST is most of the storage, so ``heap_bytes``
  alone understates a partition by an order of magnitude.
* :func:`~lightcurvedb.core.partitions.find_partition_for_value` never
  returns the DEFAULT partition. A value that would be *routed* to the
  default has no partition *for* it; ask
  :func:`~lightcurvedb.core.partitions.default_partition_of` separately.

Pre-flighting an attach
-----------------------

``ATTACH PARTITION`` takes ``ACCESS EXCLUSIVE`` on the parent. Anything
PostgreSQL has to scan or build while holding that lock -- a partition
constraint it cannot prove, an index it cannot adopt, a foreign key it
must validate, a DEFAULT partition it must check -- blocks every reader of
the table for the duration.
:func:`~lightcurvedb.core.partitions.check_attachable` predicts all of
that from the catalog first, without taking any lock.

A fully prepared candidate looks like this. Build it, load it, then index
and constrain it -- in that order, so the bulk load pays no index or
trigger cost:

.. code-block:: sql

   -- Shape from the parent; indexes come after the load.
   CREATE TABLE dataset_obs_5_v3 (
       LIKE dataset INCLUDING ALL EXCLUDING INDEXES,
       CONSTRAINT dataset_obs_5_v3_partcheck CHECK (observation_id = 5)
   );

   -- ... bulk load ...

   -- A real PRIMARY KEY constraint, not a bare unique index: PostgreSQL
   -- adopts a constraint-backed index but rebuilds a bare one under lock.
   ALTER TABLE dataset_obs_5_v3 ADD CONSTRAINT dataset_obs_5_v3_pkey
       PRIMARY KEY (observation_id, target_id,
                    photometric_method_id, processing_method_id);
   CREATE INDEX dataset_obs_5_v3_target_idx ON dataset_obs_5_v3 (target_id);

   -- Foreign keys to tables that are already live, so they validate now
   -- and are adopted at attach time instead of cloned and re-validated.
   ALTER TABLE dataset_obs_5_v3 ADD FOREIGN KEY (observation_id)
       REFERENCES observation(id) ON DELETE CASCADE;
   -- ... and the other three ...

Then ask before attaching:

.. code-block:: python

   from lightcurvedb.core.partitions import check_attachable

   report = check_attachable(conn, "dataset", "dataset_obs_5_v3", 5)

   report.ok            # True only if ATTACH would be a pure catalog change
   report.blocking      # findings that would make ATTACH fail
   report.expensive     # findings that would make ATTACH scan or build

   report.raise_for_status()                      # strict
   report.raise_for_status(allow_expensive=True)  # tolerate slow, not broken

The two categories differ in kind, not degree. **Blocking** findings --
a column mismatch, or a DEFAULT partition already holding rows for the
key -- make the ``ATTACH`` fail outright. **Expensive** findings let it
succeed, but slowly and under lock: no validated
``CHECK (<key> = <value>)`` forces a scan of every row; a missing index or
a bare index standing in for a primary key forces a build; a missing
foreign key forces a clone and validation; any DEFAULT partition forces a
scan of it.

.. warning::
   Missing foreign keys are *reported, not prescribed*. If the candidate's
   foreign keys reference the very table whose partition is about to be
   swapped out, creating them in advance pins the outgoing partition and
   the swap cannot proceed. Which tables that applies to is a decision for
   the replacement workflow, not for this check.

The one thing here that is not a pure catalog read is the probe into the
DEFAULT partition for the key. On a large default that is a real scan --
but a plain read, not one holding ``ACCESS EXCLUSIVE``, which is the
point.

Refusals
--------

The package handles LIST partitioning on a single plain column, which is
what this schema uses. RANGE and HASH partitioning, multi-column keys and
expression keys raise
:class:`~lightcurvedb.core.partitions.UnsupportedPartitionStrategyError`
rather than being guessed at; an unpartitioned table raises
:class:`~lightcurvedb.core.partitions.NotPartitionedError`; a missing
table raises :class:`~lightcurvedb.core.partitions.RelationNotFoundError`.
All derive from :class:`~lightcurvedb.core.partitions.PartitionError`.

API reference
-------------

Naming
~~~~~~

.. automodule:: lightcurvedb.core.partitions.naming
   :members:
   :undoc-members:
   :show-inheritance:

Catalog
~~~~~~~

.. automodule:: lightcurvedb.core.partitions.catalog
   :members:
   :undoc-members:
   :show-inheritance:

Errors
~~~~~~

.. automodule:: lightcurvedb.core.partitions.errors
   :members:
   :show-inheritance:
