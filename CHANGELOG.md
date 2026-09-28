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
- **Dataset Hierarchy**: New `DataSetHierarchy` model for tracking data
  lineage and processing provenance
- DataSet now supports hierarchical relationships via `source_datasets` and
  `derived_datasets` attributes
- Many-to-many self-referential relationships for complex processing
  pipelines
- Comprehensive test demonstrating QLP-style hierarchical data
  relationships in `test_dataset_relationships.py`
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
- **Helper Methods**: `DataSet.add_derived_dataset()` and
  `DataSet.add_source_dataset()` for managing hierarchy relationships
- **Sentinel Creation**: `PhotometricSource.get_or_create_unspecified()` and
  `ProcessingMethod.get_or_create_unspecified()` class methods

### Changed
- **BREAKING**: `DataSetHierarchy` now enforces intra-orbit lineage via
  `ck_datasethierarchy_intra_orbit`
  (`source_observation_id = child_observation_id`). A dataset can no
  longer be derived from one in a different observation. The table is
  partitioned on `source_observation_id`, so the invariant keeps a
  hierarchy row in the same partition as every DataSet row it
  references -- which is what allows `dataset` and `datasethierarchy` to
  be detached and reattached as one unit when an observation's data is
  replaced.
- **BREAKING**: `DataSetHierarchy.source_target_id` and `child_target_id`
  widened from `INTEGER` to `BIGINT` to match `target.id`. SQLAlchemy only
  infers a column's type from its referent when the `ForeignKey` sits on
  the column, so these table-level composite-key columns silently rendered
  `INTEGER`; a target id above 2^31 could hold a lightcurve but raised
  `integer out of range` when recording lineage. Requires a table rewrite
  on provisioned databases -- see Database Administration Notes.
- **BREAKING**: Refactored dataset processing model architecture
- **BREAKING**: Replaced `ProcessingGroup` model with direct relationships
  in `DataSet`
- **BREAKING**: Renamed `DetrendingMethod` to `ProcessingMethod` to
  broaden scope beyond detrending
- **BREAKING**: DataSet no longer has auto-increment `id` column; uses
  composite primary key instead
- **BREAKING**: DataSet `photometric_method_id` and `processing_method_id`
  are now non-nullable (use sentinel value 0 for unspecified)
- **BREAKING**: DataSetHierarchy uses composite foreign keys (8 columns)
  instead of simple id references
- **BREAKING**: PhotometricSource and ProcessingMethod `id` columns are
  no longer autoincrement; explicit IDs required
- DataSet hierarchy relationships (`source_datasets`, `derived_datasets`)
  are now `viewonly=True`; use helper methods to create links
- Updated model exports in `__init__.py` to reflect new architecture

### Fixed
- `check_attachable` probed the DEFAULT partition in its *parent's* schema
  rather than its own. The partition's name is resolved by oid, so
  re-qualifying it with the parent's schema named a relation that does not
  exist wherever the two differ, and the probe raised `UndefinedTable`
  instead of reporting whether the default holds conflicting rows

### Removed
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
- Replace `raw_dataset.derived_datasets.append(derived)` with
  `raw_dataset.add_derived_dataset(derived, session)`
- Provide explicit IDs when creating PhotometricSource and ProcessingMethod
  records (autoincrement is disabled)
- Database schema migration required:
  - Add `DataSetHierarchy` table with composite foreign keys
  - Create sentinel records (id=0) in `photometric_source` and
    `processing_method` tables
  - Create PostgreSQL LIST partitions for each observation_id
  - Widen `datasethierarchy.source_target_id` / `child_target_id` to
    `BIGINT` (see Database Administration Notes for the procedure)
  - Add `ck_datasethierarchy_intra_orbit` to `datasethierarchy` (see
    Database Administration Notes; verify the invariant holds first)

### Database Administration Notes

#### Widening `datasethierarchy` target ids

Required on any provisioned database. Step 2 rewrites the table and its
indexes under `ACCESS EXCLUSIVE`, cascading to every partition, so its cost
is proportional to table size -- size it first and schedule off-peak.

```sql
-- 1. Size the rewrite.
SELECT c.relname,
       c.reltuples::bigint                           AS est_rows,
       pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size
  FROM pg_inherits i
  JOIN pg_class c ON c.oid = i.inhrelid
 WHERE i.inhparent = to_regclass('datasethierarchy')
 ORDER BY pg_total_relation_size(c.oid) DESC;

-- 2. Widen. Cascades to all partitions.
BEGIN;
SET LOCAL lock_timeout = '5s';
ALTER TABLE datasethierarchy
    ALTER COLUMN source_target_id TYPE bigint,
    ALTER COLUMN child_target_id  TYPE bigint;
COMMIT;
```

#### Enforcing intra-orbit lineage

Step 0 is a stop condition, not a formality: a non-zero count means
lineage does span observations, the invariant is wrong for this database,
and the constraint must not be applied until that is resolved.

```sql
-- 0. PRE-FLIGHT. Must return 0.
SELECT count(*) AS cross_orbit_rows
  FROM datasethierarchy
 WHERE source_observation_id <> child_observation_id;

-- 1. Add the constraint. NOT VALID is a catalog-only change; VALIDATE
--    scans but takes only SHARE UPDATE EXCLUSIVE, so it blocks neither
--    reads nor writes.
ALTER TABLE datasethierarchy
    ADD CONSTRAINT ck_datasethierarchy_intra_orbit
    CHECK (source_observation_id = child_observation_id) NOT VALID;

ALTER TABLE datasethierarchy
    VALIDATE CONSTRAINT ck_datasethierarchy_intra_orbit;
```

#### Partition management

Partitions are now created from Python, and the call is idempotent, so it
belongs at the start of an ingestion run rather than in a DBA checklist:

```python
from lightcurvedb.core.partitions import ensure_partition

for table in ("dataset", "datasethierarchy", "target_specific_time"):
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
`datasethierarchy` together, inside one transaction. Both revisions stay
on disk until `drop_retired` is called explicitly, which is what makes
the change reviewable and reversible.
