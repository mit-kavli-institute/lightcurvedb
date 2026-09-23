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

The DataSet table requires partition management:
```sql
-- Create partitions for each observation
CREATE TABLE dataset_obs_1 PARTITION OF dataset FOR VALUES IN (1);
CREATE TABLE dataset_obs_2 PARTITION OF dataset FOR VALUES IN (2);

-- Default partition for unexpected values
CREATE TABLE dataset_default PARTITION OF dataset DEFAULT;
```
