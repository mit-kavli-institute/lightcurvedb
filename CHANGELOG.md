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

### Changed
- **BREAKING**: Refactored dataset processing model architecture
- **BREAKING**: Replaced `ProcessingGroup` model with direct relationships
  in `DataSet`
- **BREAKING**: Renamed `DetrendingMethod` to `ProcessingMethod` to
  broaden scope beyond detrending
- DataSet now has separate nullable `photometric_method_id` and
  `processing_method_id` foreign keys instead of `processing_group_id`
- Updated model exports in `__init__.py` to reflect new architecture

### Removed
- **BREAKING**: Removed `ProcessingGroup` model (use DataSet direct
  relationships instead)
- **BREAKING**: Removed `DetrendingMethod` model (renamed to
  `ProcessingMethod`)

### Migration Guide
For existing code:
- Replace `DetrendingMethod` imports with `ProcessingMethod`
- Remove `ProcessingGroup` references
- Update DataSet queries to use `photometry_source` and
  `processing_method` instead of `processing_group`
- Database schema migration required to update foreign keys and add
  `DataSetHierarchy` table
