# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
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

### Database Administration Notes
The DataSet table requires partition management:
```sql
-- Create partitions for each observation
CREATE TABLE dataset_obs_1 PARTITION OF dataset FOR VALUES IN (1);
CREATE TABLE dataset_obs_2 PARTITION OF dataset FOR VALUES IN (2);

-- Default partition for unexpected values
CREATE TABLE dataset_default PARTITION OF dataset DEFAULT;
```
