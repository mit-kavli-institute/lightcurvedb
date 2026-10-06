Partition Management
====================

``dataset`` and ``target_specific_time`` are PostgreSQL
LIST-partitioned tables keyed on an observation id, so one
observation's data lives in one partition of each. The
:mod:`lightcurvedb.core.partitions` package works with those partitions
directly: naming them, reading what is attached, building replacements
off to one side, and promoting a replacement atomically across several
tables at once.

The package knows nothing about lightcurves. It operates on relation
names and a :class:`sqlalchemy.engine.Connection`, so it works against
any LIST-partitioned table in the metadata; ``dataset`` and
``target_specific_time`` are simply the pair this project swaps together.

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

**A partition need not live with its parent.** ``schema`` names the
partitioned parent; ``partition_schema`` names where its children go, and
defaults to the parent's. A deployment that keeps every child in a schema
of its own -- so ``\dt`` stays readable -- passes ``partition_schema`` and
nothing else changes: the read paths walk ``pg_inherits`` and report each
child's real schema themselves.

**Nothing commits.** Every function takes a
:class:`sqlalchemy.engine.Connection` and leaves the transaction open.
For the reads that is merely tidy; for the swap it is the whole design,
because the caller's transaction boundary is what makes the swap atomic.
Get a connection from a session with ``session.connection()`` or from an
engine with ``engine.connect()``.

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

Where partitions live
---------------------

By default a partition is created in its parent's schema, which is what
PostgreSQL does unless a schema is named. Pass ``partition_schema`` to put
the children somewhere else:

.. code-block:: python

   ensure_partition(conn, "dataset", 5, partition_schema="_partitions")
   # CREATE TABLE _partitions.dataset_obs_5 PARTITION OF public.dataset ...

   find_partition_for_value(conn, "dataset", 5).schema   # '_partitions'

Two rules follow from that, and they are the ones worth holding on to:

* **Reading needs no argument.** Every read walks ``pg_inherits`` from the
  parent's oid, which is schema-blind, and
  :class:`~lightcurvedb.core.partitions.PartitionInfo` carries the child's
  real ``schema``. Only the functions that *write* a name into a statement
  -- or sweep ``pg_class`` for one, such as
  :func:`~lightcurvedb.core.partitions.revisions_of` -- have to be told.
* **A name is not an address.**
  :class:`~lightcurvedb.core.partitions.PartitionName` is schema-free by
  design: the same relation name in two schemas is two different
  relations, and which one you mean is an argument, not part of the name.

A swap is the one place the two sides can differ. ``partition_schema`` on
:func:`~lightcurvedb.core.partitions.plan_swap` says where the *incoming*
relations are; each retiring relation is found in the catalog and carries
its own ``retiring_schema``, so a deployment part-way through moving its
partitions can promote ``_partitions.dataset_obs_5_v1`` over a live
``public.dataset_obs_5`` in one step.

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

Provisioning an observation
---------------------------

An observation with no data yet needs a partition before anything can be
written to it. :func:`~lightcurvedb.core.partitions.ensure_partition` is
idempotent, so it belongs at the start of an ingestion run rather than in
a migration:

.. code-block:: python

   from lightcurvedb.core.partitions import ensure_partition

   for table in ("dataset", "target_specific_time"):
       ensure_partition(conn, table, observation_id)
   session.commit()

   # ... or, keeping the children in a schema of their own:
   for table in ("dataset", "target_specific_time"):
       ensure_partition(
           conn, table, observation_id, partition_schema="_partitions"
       )

This emits ``CREATE TABLE ... PARTITION OF``, which creates the child
indexes itself and needs no validation scan, and then adds a bound
``CHECK`` while the table is still empty -- the only moment that
constraint is free. It matters later: a partition keeps its ``CHECK``
when detached, and a detached partition that has one can be re-attached
without PostgreSQL re-reading every row. That is what makes rolling a
replacement back cheap.

It is the right tool for an *empty* slot and only for that: if some
other relation already holds the value, it raises rather than adopting
or replacing it, because replacing a populated partition is a swap and
needs the rest of this page.

This supersedes the manual ``CREATE TABLE ... PARTITION OF`` recipe in
the CHANGELOG. There is no DEFAULT partition in production and there
should not be one -- see :ref:`partition-refusals` below.

Staging a replacement
---------------------

A replacement is built as an ordinary standalone table while the old
partition stays live and queryable. The order matters: **create, load,
index, add foreign keys, attach**. Indexing before the load pays a
per-row maintenance cost for nothing, and attaching before indexing makes
PostgreSQL build the index while holding ``ACCESS EXCLUSIVE`` on the
parent.

.. code-block:: python

   from lightcurvedb.core.partitions import (
       analyze_relation,
       build_partition_indexes,
       ensure_staging_table,
       mirror_outbound_foreign_keys,
       next_revision,
   )

   revision = next_revision(conn, "dataset", 5)      # 1, if v0 is live
   staged = ensure_staging_table(conn, "dataset", 5, revision)
   staged.table                                      # 'dataset_obs_5_v1'

   # Every function on this page takes ``partition_schema`` alongside
   # ``schema``; pass it consistently or the sweeps disagree about which
   # revisions exist.

   # ... bulk load into staged.table ...

   build_partition_indexes(conn, "dataset", staged.table)
   mirror_outbound_foreign_keys(conn, "dataset", staged.table)
   analyze_relation(conn, staged.table)

Each step is doing something specific and slightly counter-intuitive:

* **The staging table is built with ``LIKE ... INCLUDING ALL EXCLUDING
  INDEXES``**, read from the live catalog rather than from SQLAlchemy
  metadata, so it picks up anything a DBA added out of band. The
  bound-implying ``CHECK`` is declared inline, which makes it
  ``convalidated`` from birth -- a validated CHECK is what lets ``ATTACH``
  skip reading every row.
* **Loading goes into the standalone table, never through the partitioned
  parent**, which is 18-75x faster: no tuple routing and no per-row
  foreign-key triggers.
* **The primary key is added as a constraint, not as a unique index.**
  PostgreSQL adopts a child index at attach time only when it is backed
  by a constraint of the same kind as the parent's. A
  matching-but-constraintless unique index is rejected and rebuilt inside
  the swap window.
* **Only foreign keys pointing at unpartitioned tables are mirrored.** A
  validated key is *adopted* rather than re-verified at attach time,
  which is worth two orders of magnitude -- but see the warning below for
  why the rule stops there.
* **``ANALYZE`` is not optional.** A freshly attached partition with no
  statistics draws default selectivity estimates, and plans that used to
  nested-loop can flip to sequential scans the moment it goes live.

.. warning::
   :func:`~lightcurvedb.core.partitions.mirror_outbound_foreign_keys`
   skips keys that point at a **partitioned** table, and that default is
   the only safe setting during a replacement. Such a key on a detached
   staging table is still a real dependency on the referent's live
   partitions: PostgreSQL then refuses to detach the partition being
   replaced, and the swap cannot proceed. Since PostgreSQL 14 also
   rejects ``NOT VALID`` foreign keys on partitioned tables, there is no
   way to pre-validate them either. For a table whose keys point at
   ``dataset``, that validation therefore happens inside the swap window
   and dominates it -- budget roughly 0.8 microseconds per row. No table
   in the current schema has such keys.

Pre-flighting an attach
-----------------------

``ATTACH PARTITION`` takes ``ACCESS EXCLUSIVE`` on the parent. Anything
PostgreSQL has to scan or build while holding that lock -- a partition
constraint it cannot prove, an index it cannot adopt, a foreign key it
must validate, a DEFAULT partition it must check -- blocks every reader of
the table for the duration.
:func:`~lightcurvedb.core.partitions.check_attachable` predicts all of
that from the catalog first, without taking any lock.

.. code-block:: python

   from lightcurvedb.core.partitions import check_attachable

   report = check_attachable(conn, "dataset", "dataset_obs_5_v1", 5)

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

The one thing here that is not a pure catalog read is the probe into the
DEFAULT partition for the key. On a large default that is a real scan --
but a plain read, not one holding ``ACCESS EXCLUSIVE``, which is the
point.

Promoting a replacement
-----------------------

Promotion detaches the old partitions and attaches the new ones for
every table involved, inside one transaction, so an observation's data
changes all at once.

.. code-block:: python

   from lightcurvedb.core.partitions import plan_swap, preflight, swap

   plan = plan_swap(
       conn,
       [("dataset", "dataset_obs_5_v1"),
        ("target_specific_time", "target_specific_time_obs_5_v1")],
       5,
       # partition_schema="_partitions",   # where the incoming relations are
   )

   report = preflight(conn, plan)
   report.blocking          # must be empty
   report.expensive         # read this before swapping
   report.holders           # backends already holding a lock on a parent

   result = swap(conn, plan)
   session.commit()         # nothing is durable until you do this

   result.promoted  # ('dataset_obs_5_v1', 'target_specific_time_obs_5_v1')
   result.retired   # ('dataset_obs_5',    'target_specific_time_obs_5')

:func:`~lightcurvedb.core.partitions.plan_swap` resolves what is live and
**derives the order from foreign keys in the catalog**: a parent that
references another is detached first and attached last. No visible
instant then has a row referencing a partition that is not there, and the
referencing side validates against keys that are already in place. A
third table added to the list sorts itself.

:func:`~lightcurvedb.core.partitions.swap` bounds both timeouts, takes
every lock up front in one canonical order, re-checks under that lock
that the partitions it plans to retire are still the live ones, performs
the four statements, and finally gives each retired relation its bound
``CHECK`` back. The whole critical section is O(1) in row count apart
from validating any foreign key that points at a partitioned table, and
the current schema has none.

Two details in that sequence are worth stating plainly:

* **``lock_timeout`` and ``statement_timeout`` are both set.** The first
  bounds how long the swap *waits*, the second how long it *holds*.
  Setting only the first is a trap: a swap that eventually acquires
  ``ACCESS EXCLUSIVE`` can then hold it indefinitely, with every reader
  that arrived meanwhile queued behind it.
* **The swap adds no constraints.** A retired partition wants a bound
  ``CHECK`` -- without one, re-attaching it during a rollback re-reads
  every row -- but validating a ``CHECK`` *is* a scan, and doing it here
  would mean scanning with every parent locked. Partitions this package
  provisioned already carry the constraint. For one created by hand,
  :func:`~lightcurvedb.core.partitions.prepare_retirement` adds it
  beforehand, outside the swap's transaction, where the scan locks only
  that partition:

  .. code-block:: python

     from lightcurvedb.core.partitions import prepare_retirement

     prepare_retirement(conn, plan)   # own transaction, before the swap
     session.commit()

  ``report.unprotected_retirements`` names the partitions that need it,
  and the same finding appears in ``report.expensive``.

If the lock cannot be taken in time PostgreSQL raises SQLSTATE
``55P03`` and the transaction is dead.
:func:`~lightcurvedb.core.partitions.is_lock_not_available` recognises
it and :func:`~lightcurvedb.core.partitions.blocking_pids` says who is in
the way; retrying is the caller's decision, because only the caller knows
whether its transaction can be replayed. Note also that ``ACCESS
EXCLUSIVE`` replays on hot standbys and will cancel conflicting queries
there according to ``max_standby_streaming_delay``.

Rolling back and retiring
-------------------------

There are three undos, and which one applies depends only on where you
are:

**Before the commit**, ``session.rollback()`` is the entire undo. That is
the reason promotion is one transaction rather than a sequence of
concurrent operations.

**After the commit, while the retired relations still exist**,
:func:`~lightcurvedb.core.partitions.rollback_swap` performs the mirror
swap. This window is exactly the coexistence the design is for: both
revisions are on disk, the old one is a query away, and restoring it
costs about what the promotion did.

.. code-block:: python

   from lightcurvedb.core.partitions import rollback_swap

   # The old data is readable throughout, by name:
   session.execute(sa.text("SELECT count(*) FROM dataset_obs_5"))

   rollback_swap(conn, result)
   session.commit()

**After the retired relations are dropped**, there is no undo but a
restore from backup. :func:`~lightcurvedb.core.partitions.drop_retired`
is therefore dry-run by default, refuses anything still attached, and
takes an explicit list:

.. code-block:: python

   from lightcurvedb.core.partitions import drop_retired

   drop_retired(conn, result.retired, schema=result.partition_schema)
   # lists, drops nothing; add dry_run=False to make it irreversible

Nothing drops a retired partition automatically, and nothing here knows
whether a revision was signed off -- that record belongs in a registry,
and this package does not read one. An observation under replacement
therefore uses roughly twice its usual storage until somebody decides
otherwise.

The lifecycle
-------------

.. mermaid::

   stateDiagram-v2
       [*] --> ABSENT
       ABSENT --> CREATED: ensure_staging_table
       CREATED --> LOADED: bulk load
       LOADED --> INDEXED: build_partition_indexes
       INDEXED --> FK_READY: mirror_outbound_foreign_keys
       FK_READY --> VERIFIED: sign-off
       VERIFIED --> LIVE: swap
       FK_READY --> LIVE: swap
       LIVE --> RETIRED: swap (superseded)
       LIVE --> DETACH_PENDING: interrupted concurrent detach
       DETACH_PENDING --> RETIRED: detach_partition(finalize=True)
       RETIRED --> LIVE: rollback_swap
       RETIRED --> GONE: drop_retired
       CREATED --> ABSENT: abandoned
       LOADED --> ABSENT: abandoned
       INDEXED --> ABSENT: abandoned
       FK_READY --> ABSENT: abandoned
       GONE --> [*]

:func:`~lightcurvedb.core.partitions.derive_state` reads a revision's
state out of the catalog, and
:func:`~lightcurvedb.core.partitions.validate_transition` refuses a move
with no edge. Three of the states are not physical and cannot be derived:
``VERIFIED`` records that a person looked at the data, ``RETIRED``
differs from a fully prepared staging table by history alone -- a
detached relation is byte-for-byte what it was while attached -- and
``GONE`` differs from ``ABSENT`` only in that the relation once existed.
Those come from a registry, which is why one exists.

Because state is derived rather than stored, a process that dies
mid-campaign learns where it got to by looking, and every step can simply
be run again.

Later replacements
------------------

Replacement is a recurring capability, not a one-off repair. The same
mechanism serves a correction, a reprocessing, or a deliberate downgrade
to less precise photometry, and it has to stay correct when the next one
happens years later against partitions this code has never seen.

**Revisions are monotonic per slot.**
:func:`~lightcurvedb.core.partitions.next_revision` reads every relation
whose name parses as a revision of that observation -- live, staged or
retired -- and returns one past the highest. A partition a DBA created by
hand reads as revision 0, so the first replacement is v1 whether or not
this package provisioned what came before.

**Newer is not better.** Nothing in the design treats a higher revision
as more correct: promotion is explicit, and a revision that turns out to
be wrong is rolled back to a *named* earlier one rather than to "the
previous one". That is what keeping every retired revision until someone
drops it buys.

.. code-block:: python

   from lightcurvedb.core.partitions import revisions_of, live_revision

   revisions_of(conn, "dataset", 5)
   # {0: 'dataset_obs_5', 1: 'dataset_obs_5_v1', 2: 'dataset_obs_5_v2'}

   live_revision(conn, "dataset", 5)      # 2

**Tables swapped together must stay together.**
:func:`~lightcurvedb.core.partitions.assert_paired_revisions` raises if
``dataset`` is live at one revision while ``target_specific_time`` is
live at another. Nothing in normal operation can produce that -- it means a swap
was not atomic -- so it is reported loudly rather than repaired.
:func:`~lightcurvedb.core.partitions.preflight` checks it before every
promotion.

.. _partition-refusals:

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

Several refusals exist specifically to stop a plausible-looking mistake:

* :class:`~lightcurvedb.core.partitions.StagingShapeMismatchError` --
  ``CREATE TABLE IF NOT EXISTS`` accepts a staging table left over from
  an older definition of the parent, and the mismatch would otherwise
  surface at attach time, under lock.
* :class:`~lightcurvedb.core.partitions.AutocommitRequiredError` --
  ``DETACH PARTITION ... CONCURRENTLY`` cannot run in a transaction
  block. Raising before the statement is sent keeps the caller's
  transaction usable.
* :class:`~lightcurvedb.core.partitions.SwapRaceError` -- the live
  partition changed between planning and swapping, so somebody else's
  promotion would be discarded.
* :class:`~lightcurvedb.core.partitions.RevisionSkewError` -- paired
  tables are live at different revisions.
* :class:`~lightcurvedb.core.partitions.PartitionError` from
  :func:`~lightcurvedb.core.partitions.rollback_swap` -- the relations a
  swap retired are spread across two schemas, and one plan carries one
  ``partition_schema``, so no single rollback can address them all.

.. warning::
   A DEFAULT partition is convenient in a test database and harmful in a
   production one. While one exists, every ``ATTACH`` must scan it to
   prove none of its rows belong in the incoming partition -- under
   ``ACCESS EXCLUSIVE``, and it fails outright if any row does -- and
   ``DETACH PARTITION ... CONCURRENTLY`` is refused entirely. Rows that
   land in a default are also invisible to this machinery, which works
   one bound at a time.
   :func:`~lightcurvedb.core.partitions.ensure_default_partition` exists
   for tests and should not be used against a real database.

API reference
-------------

Naming
~~~~~~

.. automodule:: lightcurvedb.core.partitions.naming
   :members:
   :show-inheritance:

Catalog
~~~~~~~

.. automodule:: lightcurvedb.core.partitions.catalog
   :members:
   :show-inheritance:

Lifecycle state
~~~~~~~~~~~~~~~

.. automodule:: lightcurvedb.core.partitions.state
   :members:
   :show-inheritance:

DDL primitives
~~~~~~~~~~~~~~

.. automodule:: lightcurvedb.core.partitions.ddl
   :members:
   :show-inheritance:

Provisioning
~~~~~~~~~~~~

.. automodule:: lightcurvedb.core.partitions.bootstrap
   :members:
   :show-inheritance:

The swap
~~~~~~~~

.. automodule:: lightcurvedb.core.partitions.swap
   :members:
   :show-inheritance:

Errors
~~~~~~

.. automodule:: lightcurvedb.core.partitions.errors
   :members:
   :show-inheritance:
