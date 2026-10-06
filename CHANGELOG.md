# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **Partition naming**: `lightcurvedb.core.partitions` with `PartitionName`,
  the canonical `<base>_obs_<observation_id>_v<revision>` scheme (revision
  0 is the legacy `<base>_obs_<id>` form), a 63-byte identifier check on
  every derived object name, and the `PartitionError` exception hierarchy
- **Partition introspection**: `lightcurvedb.core.partitions.catalog` reads
  `pg_catalog` for a table's partition strategy and key, its attached
  partitions with bounds and sizes, and the partition holding a given
  value. `check_attachable` predicts -- without taking a lock -- whether
  `ATTACH PARTITION` would fail, or succeed only by scanning or building
  under `ACCESS EXCLUSIVE`. Also exposes `column_signature`,
  `index_definitions` and `foreign_key_definitions` for comparing a
  candidate against its parent
- **Partition DDL**: `lightcurvedb.core.partitions.ddl` emits the
  statements a replacement needs -- `create_staging_table` (a `LIKE ...
  INCLUDING ALL EXCLUDING INDEXES` copy carrying an inline, already-valid
  bound `CHECK`), `build_partition_indexes` (primary keys as real
  constraints, so `ATTACH` adopts them instead of rebuilding under
  `ACCESS EXCLUSIVE`), `mirror_outbound_foreign_keys`, `attach_partition`,
  `detach_partition`, `add_bound_check`, `drop_relation`, plus
  `set_local_timeouts`, `lock_tables` and `referenced_tables`
- **Idempotent provisioning**: `lightcurvedb.core.partitions.bootstrap`
  with `ensure_partition`, `ensure_staging_table` and
  `verify_relation_shape`. Safe to call on every process start -- this is
  what replaces a migration tool for partition creation
- **Partition lifecycle**: `lightcurvedb.core.partitions.state` derives a
  revision's state from `pg_catalog` (`derive_state`), reports which
  revision is live (`live_revision`, `revisions_of`), allocates the next
  one (`next_revision`, monotonic per observation across campaigns), and
  refuses paired tables drifting apart (`assert_paired_revisions`)
- **Partitions in a schema of their own**: every partition-addressing
  function takes `partition_schema` alongside `schema`, defaulting to the
  parent's, so a deployment can keep its children in a dedicated schema
  while the partitioned parents stay where they are. Reads need no
  argument -- they walk `pg_inherits`, and `PartitionInfo` reports each
  child's real schema. `SwapPlan` and `SwapResult` record where the
  incoming and promoted relations live; each `SwapPair` carries the
  schema of the relation it retires, read from the catalog, so a swap can
  promote a partition in one schema over a live one in another
- **Atomic multi-table swap**: `lightcurvedb.core.partitions.swap`
  promotes staged partitions for several tables in one transaction.
  `plan_swap` derives the detach/attach order from foreign keys in the
  catalog, `preflight` predicts the cost without taking a lock, `swap`
  bounds both timeouts and re-checks under lock what it is retiring,
  `prepare_retirement` gives a hand-made partition its bound `CHECK`
  before the swap rather than during it, `rollback_swap` performs the
  mirror swap while the retired relations still exist, and
  `drop_retired` is dry-run by default
- **PostgreSQL LIST Partitioning**: DataSet table now uses PostgreSQL LIST
  partitioning by `observation_id` for efficient query performance at scale
  (billions of rows, terabytes of data)
- **Composite Primary Key**: DataSet uses composite primary key
  `(observation_id, target_id, photometric_method_id, processing_method_id)`
  for natural partitioning alignment
- **Sentinel Values**: `PhotometricSource.UNSPECIFIED_ID` and
  `ProcessingMethod.UNSPECIFIED_ID` (id=0) for "unspecified" records,
  replacing NULL values in composite key columns
- **Hybrid Properties**: `DataSet.has_photometric_source` and
  `DataSet.has_processing_method` for filtering datasets with/without
  specific sources or methods (works in both Python and SQL queries)
- **Sentinel Creation**: `PhotometricSource.get_or_create_unspecified()` and
  `ProcessingMethod.get_or_create_unspecified()` class methods

### Changed
- **BREAKING**: Refactored dataset processing model architecture
- **BREAKING**: Replaced `ProcessingGroup` model with direct relationships
  in `DataSet`
- **BREAKING**: Renamed `DetrendingMethod` to `ProcessingMethod` to
  broaden scope beyond detrending
- **BREAKING**: DataSet no longer has auto-increment `id` column; uses
  composite primary key instead
- **BREAKING**: DataSet `photometric_method_id` and `processing_method_id`
  are now non-nullable (use sentinel value 0 for unspecified)
- **BREAKING**: PhotometricSource and ProcessingMethod `id` columns are
  no longer autoincrement; explicit IDs required
- Updated model exports in `__init__.py` to reflect new architecture

### Fixed
- `foreign_key_definitions` reported the foreign keys PostgreSQL clones
  onto a referencing table, one per partition of a referenced partitioned
  table, as if they were that table's own. Each clone points at a
  concrete partition, so it reads as unpartitioned and escaped the
  partitioned-referent skip: `mirror_outbound_foreign_keys` then tried to
  copy it onto a staging table, which would pin the partition a swap has
  to detach, and failed first on the 63-byte identifier limit because the
  clones' generated names are already at it. Only constraints with
  `conparentid = 0` -- a table's own declarations -- are reported now.
  This affected any table with foreign keys into a partitioned table,
  as soon as that table had a partition
- `check_attachable` probed the DEFAULT partition in its *parent's* schema
  rather than its own. The partition's name is resolved by oid, so
  re-qualifying it with the parent's schema named a relation that does not
  exist wherever the two differ, and the probe raised `UndefinedTable`
  instead of reporting whether the default holds conflicting rows

### Removed
- **BREAKING**: Removed the `DataSetHierarchy` model and the
  `datasethierarchy` table, together with `DataSet.source_datasets`,
  `DataSet.derived_datasets`, `DataSet.add_derived_dataset()` and
  `DataSet.add_source_dataset()`. The table was populated but nothing
  downstream ever read it. Keeping it cost more than it was worth: it was
  the only table with foreign keys into the partitioned `dataset`, so
  every write to it locked every `dataset` partition to check those keys,
  and every partition swap had to validate them under `ACCESS EXCLUSIVE`.
  The `BIGINT` widening of its target ids and the
  `ck_datasethierarchy_intra_orbit` constraint, both added for it earlier
  in this release cycle and never released, are withdrawn with it. Neither
  needs to be applied to a provisioned database. See Database
  Administration Notes for dropping the table
- **BREAKING**: Removed `ProcessingGroup` model (use DataSet direct
  relationships instead)
- **BREAKING**: Removed `DetrendingMethod` model (renamed to
  `ProcessingMethod`)
- **BREAKING**: Removed DataSet.id auto-increment primary key

### Migration Guide
For existing code:
- Replace `DetrendingMethod` imports with `ProcessingMethod`
- Remove `ProcessingGroup` references
- Update DataSet queries to use `photometry_source` and
  `processing_method` instead of `processing_group`
- Replace `processing_method=None` with
  `processing_method_id=ProcessingMethod.UNSPECIFIED_ID`
- Replace `photometry_source=None` with
  `photometric_method_id=PhotometricSource.UNSPECIFIED_ID`
- Remove uses of `DataSetHierarchy`, `DataSet.source_datasets`,
  `DataSet.derived_datasets`, `add_derived_dataset()` and
  `add_source_dataset()`. Nothing replaces them, and any process that
  still writes lineage must stop before the table is dropped
- Provide explicit IDs when creating PhotometricSource and ProcessingMethod
  records (autoincrement is disabled)
- Database schema migration required:
  - Create sentinel records (id=0) in `photometric_source` and
    `processing_method` tables
  - Create PostgreSQL LIST partitions for each observation_id
  - Drop the `datasethierarchy` table (see Database Administration Notes)

### Database Administration Notes

#### Dropping `datasethierarchy`

The table is no longer mapped, so 3.3.0 neither reads nor writes it. It
has to be dropped by hand, and in stages. With thousands of partitions, a
single `DROP TABLE datasethierarchy` runs out of lock-table memory
(`HINT: You might need to increase max_locks_per_transaction`). Measured
on PostgreSQL 14:

- A plain `DROP TABLE` on the parent takes about **7 locks per `dataset`
  partition** in one transaction. Each of its two foreign keys has a copy,
  with triggers, on every `dataset` partition, and they all go at once.
- Dropping the hierarchy partitions one at a time takes about 6 locks
  each, regardless of scale.
- Dropping the parent's two foreign keys separately takes about **4 locks
  per `dataset` partition** each. That is the peak, and it cannot be
  split further, so step 4 below is the only step that may need a
  setting change.

Step 0 is a stop condition: nothing may still be writing lineage.

```sql
-- 0. No writers left. Every process that called add_derived_dataset() /
--    add_source_dataset() must be on 3.3.0 or have that code removed.
SELECT count(*) AS live_partitions
  FROM pg_inherits WHERE inhparent = to_regclass('datasethierarchy');
```

```bash
# 1. Optional: archive the data, one partition at a time (a COPY of the
#    parent would lock every partition at once).
psql -d lightcurvedb -Atc "SELECT c.oid::regclass FROM pg_inherits i
    JOIN pg_class c ON c.oid = i.inhrelid
    WHERE i.inhparent = to_regclass('datasethierarchy')" |
while read -r t; do
    psql -d lightcurvedb -c "\copy $t TO '$t.csv' CSV HEADER"
done
```

```sql
-- 2. Drop the partitions. Run with psql in autocommit mode (the default):
--    each generated DROP is its own transaction and takes ~6 locks.
--    psql -d lightcurvedb -v parent=datasethierarchy -f drop_partitions.sql
SELECT format('DROP TABLE %s', c.oid::regclass)
  FROM pg_inherits i
  JOIN pg_class c ON c.oid = i.inhrelid
 WHERE i.inhparent = to_regclass(:'parent')
 ORDER BY c.relname
\gexec

-- 3. Size the peak: ~4 locks per dataset partition.
SELECT count(*)     AS dataset_partitions,
       4 * count(*) AS peak_locks
  FROM pg_inherits WHERE inhparent = to_regclass('dataset');
SELECT current_setting('max_locks_per_transaction')::int
       * (current_setting('max_connections')::int
          + current_setting('max_prepared_transactions')::int)
       AS lock_table_capacity;
--    If capacity is below ~1.25 x peak_locks, raise max_locks_per_transaction
--    to at least 5 x dataset_partitions / (max_connections +
--    max_prepared_transactions) and restart PostgreSQL first. (4,000 dataset
--    partitions with 100 connections: 192 succeeded where 128 failed.)

-- 4. Drop the foreign keys one at a time, then the empty parent. Each
--    ALTER takes ACCESS EXCLUSIVE on every dataset partition. The work is
--    catalog-only and quick, but while it waits for a running query on
--    dataset, every new query on dataset queues behind it. The timeout
--    makes it give up instead; rerun it if it does. Run off-peak.
BEGIN;
SET LOCAL lock_timeout = '5s';
ALTER TABLE datasethierarchy DROP CONSTRAINT fk_datasethierarchy_source;
COMMIT;

BEGIN;
SET LOCAL lock_timeout = '5s';
ALTER TABLE datasethierarchy DROP CONSTRAINT fk_datasethierarchy_child;
COMMIT;

DROP TABLE datasethierarchy;
```

Nothing else changes. `dataset` keeps every row and every partition.

#### Partition management

Partitions are now created from Python, and the call is idempotent, so it
belongs at the start of an ingestion run rather than in a DBA checklist:

```python
from lightcurvedb.core.partitions import ensure_partition

for table in ("dataset", "target_specific_time"):
    ensure_partition(session.connection(), table, observation_id)
session.commit()
```

That emits the SQL below, which is what the manual recipe always was:

```sql
CREATE TABLE dataset_obs_1 PARTITION OF dataset FOR VALUES IN (1);
```

**Default partitions should be dropped from provisioned databases.**
They were previously recommended here as a catch-all. While one exists,
every `ATTACH PARTITION` must scan it under `ACCESS EXCLUSIVE` to prove
none of its rows belong in the incoming partition -- and fails outright
if any do -- and `DETACH PARTITION ... CONCURRENTLY` is refused entirely.
Check for accumulated rows before dropping:

```sql
-- 0. Anything in here has no partition of its own. Resolve it first.
SELECT observation_id, count(*)
  FROM dataset_default GROUP BY observation_id ORDER BY 2 DESC;

DROP TABLE dataset_default;
```

#### Replacing an observation's data

See `docs/source/partitioning.rst` for the full procedure. In outline: a
replacement is staged as a standalone table alongside the live
partition, loaded, indexed and analysed, then promoted by detaching the
old partitions and attaching the new ones for `dataset` and
`target_specific_time` together, inside one transaction. Both revisions stay
on disk until `drop_retired` is called explicitly, which is what makes
the change reviewable and reversible.
