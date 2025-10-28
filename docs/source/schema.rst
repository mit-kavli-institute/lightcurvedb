Database Schema
===============

The LightcurveDB schema is designed to efficiently store and retrieve astronomical time-series data from the TESS mission. The schema uses PostgreSQL with SQLAlchemy ORM, supporting complex relationships and polymorphic models.

Schema Overview
---------------

The database schema consists of several interconnected model groups:

1. **Mission Hierarchy**: Mission → MissionCatalog → Target
2. **Instrument Hierarchy**: Self-referential instrument tree
3. **Observation System**: Polymorphic observation models with FITS frame support
4. **Processing Pipeline**: PhotometricSource and ProcessingMethod
5. **Data Products**: DataSet (lightcurves), TargetSpecificTime, and QualityFlagArray
6. **Data Lineage**: DataSetHierarchy for tracking processing provenance

Entity Relationship Diagram
---------------------------

.. mermaid::
   :caption: LightcurveDB Entity Relationship Diagram
   :alt: ER diagram showing relationships between Mission, Target, Observation, and data processing models
   :align: center

   ---
   config:
     theme: neutral
     layout: elk
     elk:
       mergeEdges: true
       nodePlacementStrategy: LINEAR_SEGMENTS
   ---
   erDiagram
       Mission ||--o{ MissionCatalog : "has catalogs"
       MissionCatalog ||--o{ Target : "contains targets"

       Instrument ||--o{ Instrument : "parent/child"
       Instrument ||--o{ Observation : "produces"

       Observation ||--o{ FITSFrame : "polymorphic"
       Observation ||--o{ TargetSpecificTime : "has times"
       Observation ||--o{ DataSet : "has datasets"
       Observation ||--o{ QualityFlagArray : "has quality flags"

       Target ||--o{ TargetSpecificTime : "has times"
       Target ||--o{ DataSet : "has lightcurves"
       Target ||--o{ QualityFlagArray : "has quality flags"

       PhotometricSource ||--o{ DataSet : "extracts data for"
       ProcessingMethod ||--o{ DataSet : "processes data for"
       DataSet ||--o{ DataSetHierarchy : "source"
       DataSet ||--o{ DataSetHierarchy : "derived"

       Mission {
           UUID id PK
           string name UK
           string description
           string time_unit
           decimal time_epoch
           string time_epoch_scale
           string time_epoch_format
           string time_format_name UK
       }

       MissionCatalog {
           int id PK
           UUID host_mission_id FK
           string name UK
           string description
       }

       Target {
           bigint id PK
           int catalog_id FK
           bigint name
       }

       Instrument {
           UUID id PK
           string name
           json properties
           UUID parent_id FK
       }

       Observation {
           int id PK
           string type
           array cadence_reference
           UUID instrument_id FK
       }

       FITSFrame {
           int id PK
           string type
           bigint cadence
           int observation_id FK
           bool simple
           int bitpix
           int naxis
           array naxis_values
           bool extended
           float bscale
           float bzero
           path file_path
       }

       PhotometricSource {
           int id PK
           string name UK
           string description
       }

       ProcessingMethod {
           int id PK
           string name UK
           string description
       }

       DataSet {
           int id PK
           int target_id FK
           int observation_id FK
           int photometric_method_id FK "nullable"
           int processing_method_id FK "nullable"
           array values
           array errors
       }

       DataSetHierarchy {
           int id PK
           int source_dataset_id FK
           int child_dataset_id FK
       }

       TargetSpecificTime {
           bigint id PK
           int target_id FK
           int observation_id FK
           array barycentric_julian_dates
       }

       QualityFlagArray {
           bigint id PK
           string type
           int observation_id FK
           int target_id FK
           array quality_flags
           datetime created_on
       }


Model Descriptions
------------------

Mission Models
~~~~~~~~~~~~~~

.. autoclass:: lightcurvedb.models.Mission
   :members:
   :show-inheritance:
   :no-index:

.. autoclass:: lightcurvedb.models.MissionCatalog
   :members:
   :show-inheritance:
   :no-index:

.. autoclass:: lightcurvedb.models.Target
   :members:
   :show-inheritance:
   :no-index:

Instrument Model
~~~~~~~~~~~~~~~~

.. autoclass:: lightcurvedb.models.Instrument
   :members:
   :show-inheritance:
   :no-index:

Observation Models
~~~~~~~~~~~~~~~~~~

.. autoclass:: lightcurvedb.models.Observation
   :members:
   :show-inheritance:
   :no-index:

.. autoclass:: lightcurvedb.models.TargetSpecificTime
   :members:
   :show-inheritance:
   :no-index:

Frame Models
~~~~~~~~~~~~

.. autoclass:: lightcurvedb.models.FITSFrame
   :members:
   :show-inheritance:
   :no-index:

Processing Models
~~~~~~~~~~~~~~~~~

.. autoclass:: lightcurvedb.models.PhotometricSource
   :members:
   :show-inheritance:
   :no-index:

.. autoclass:: lightcurvedb.models.ProcessingMethod
   :members:
   :show-inheritance:
   :no-index:

Data Product Models
~~~~~~~~~~~~~~~~~~~

.. autoclass:: lightcurvedb.models.DataSet
   :members:
   :show-inheritance:
   :no-index:

.. autoclass:: lightcurvedb.models.DataSetHierarchy
   :members:
   :show-inheritance:
   :no-index:

Quality Flag Model
~~~~~~~~~~~~~~~~~~

.. autoclass:: lightcurvedb.models.QualityFlagArray
   :members:
   :show-inheritance:
   :no-index:


Polymorphic Models
------------------

The schema uses SQLAlchemy's polymorphic inheritance for flexibility:

1. **Observation**: Base class with polymorphic_on="type"

   - Allows different observation types to share common attributes
   - Subclasses can add specialized fields while maintaining relationships

2. **FITSFrame**: Configured for polymorphism with polymorphic_on="type"

   - Supports different FITS frame types (e.g., science frames, calibration frames)
   - Identity "basefits" serves as the default type

3. **QualityFlagArray**: Base class with polymorphic_on="type"

   - Enables mission-specific quality flag implementations
   - Identity "base_quality_flag" serves as the default type
   - Can be extended to add mission-specific bit interpretations

Mission-Specific Extensions
---------------------------

LightcurveDB supports mission-specific data through SQLAlchemy's polymorphic inheritance.
The Observation model serves as a base class that can be extended for specific missions.

Design Pattern
~~~~~~~~~~~~~~

To add support for a new mission:

1. Create a subclass of Observation
2. Set a unique polymorphic_identity
3. Add mission-specific fields as Mapped columns
4. Register in your mission's module

Example: TESS Observations
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from lightcurvedb.models import Observation
   from sqlalchemy import orm

   class TESSObservation(Observation):
       """TESS-specific observation with orbit and sector information."""

       __mapper_args__ = {
           "polymorphic_identity": "tess_observation",
       }

       # TESS-specific fields
       sector: orm.Mapped[int]
       orbit_number: orm.Mapped[int]
       spacecraft_quaternion: orm.Mapped[dict]  # Store as JSON
       cosmic_ray_mitigation: orm.Mapped[bool]

Example: HST Observations
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   class HSTObservation(Observation):
       """Hubble Space Telescope observation with visit information."""

       __mapper_args__ = {
           "polymorphic_identity": "hst_observation",
       }

       visit_id: orm.Mapped[str]
       program_id: orm.Mapped[int]
       filter_name: orm.Mapped[str]
       exposure_time: orm.Mapped[float]

Querying Mission-Specific Data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Query all observations (any mission)
   all_obs = session.query(Observation).all()

   # Query only TESS observations
   tess_obs = session.query(TESSObservation).filter_by(sector=1).all()

   # Polymorphic loading - automatically returns correct subclass
   obs = session.query(Observation).first()
   if isinstance(obs, TESSObservation):
       print(f"TESS Sector: {obs.sector}")

Benefits
~~~~~~~~

- **Type Safety**: Mission-specific fields are properly typed
- **Clean Schema**: No unused fields for other missions
- **Extensibility**: New missions don't require schema changes
- **Performance**: Single table inheritance is efficient
- **Flexibility**: Can query all observations or mission-specific ones

Key Relationships
-----------------

**One-to-Many**:

- Mission → MissionCatalog → Target (hierarchical)
- Instrument → Observation
- PhotometricSource → DataSet
- ProcessingMethod → DataSet
- Observation → QualityFlagArray
- Target → QualityFlagArray (optional relationship)

**Many-to-Many** (via junction tables):

- Target ↔ Observation (via TargetSpecificTime)
- DataSet ↔ DataSet (via DataSetHierarchy for lineage tracking)

**Self-Referential**:

- Instrument parent/child hierarchy for complex instrument configurations
- DataSet hierarchy via DataSetHierarchy (source_datasets ↔ derived_datasets)

**Central Hub**:

- DataSet connects Target + Observation + PhotometricSource + ProcessingMethod
- This is where the actual lightcurve data resides
- Supports hierarchical relationships for tracking data provenance

Usage Examples
--------------

Querying for a target's lightcurves:

.. code-block:: python

   from lightcurvedb.models import Target, DataSet

   # Get all lightcurves for a specific TIC ID
   target = session.query(Target).filter_by(name=12345678).first()
   lightcurves = target.datasets

   # Get lightcurves with specific processing
   for lc in lightcurves:
       photometry = lc.photometry_source.name if lc.photometry_source else "N/A"
       processing = lc.processing_method.name if lc.processing_method else "Raw"
       print(f"Photometry: {photometry}, Processing: {processing}")
       print(f"Values: {lc.values}")

Creating instrument hierarchy:

.. code-block:: python

   from lightcurvedb.models import Instrument

   # Create camera with CCDs
   camera = Instrument(name="TESS Camera 1")
   ccd1 = Instrument(name="CCD 1", parent=camera)
   ccd2 = Instrument(name="CCD 2", parent=camera)

   session.add_all([camera, ccd1, ccd2])

Working with quality flags:

.. code-block:: python

   from lightcurvedb.models import QualityFlagArray, Target, Observation
   import numpy as np

   # Get quality flags for a specific target observation
   target = session.query(Target).filter_by(name=12345678).first()
   observation = target.observations[0]

   # Get quality flags for this target in this observation
   quality_flags = session.query(QualityFlagArray).filter_by(
       observation=observation,
       target=target,
       type="base_quality_flag"
   ).first()

   if quality_flags:
       # Check for cosmic ray events (bit 0)
       cosmic_ray_mask = (quality_flags.quality_flags & 1) != 0
       num_cosmic_rays = np.sum(cosmic_ray_mask)
       print(f"Found {num_cosmic_rays} cadences with cosmic ray events")

       # Check for saturated pixels (bit 1)
       saturation_mask = (quality_flags.quality_flags & 2) != 0
       print(f"Saturated in {np.sum(saturation_mask)} cadences")

Tracking data lineage with dataset hierarchy:

.. code-block:: python

   from lightcurvedb.models import DataSet
   import numpy as np

   # Create raw photometry dataset
   raw_flux = np.random.normal(1.0, 0.01, 1000)
   raw_dataset = DataSet(
       target=target,
       observation=observation,
       photometry_source=aperture_source,
       processing_method=None,  # Raw data, no processing
       values=raw_flux
   )
   session.add(raw_dataset)
   session.flush()

   # Create detrended dataset derived from raw
   detrended_flux = raw_flux - np.polyval([0.001, 0], range(1000))
   detrended_dataset = DataSet(
       target=target,
       observation=observation,
       photometry_source=aperture_source,
       processing_method=detrending_method,
       values=detrended_flux
   )

   # Establish hierarchical relationship
   detrended_dataset.source_datasets.append(raw_dataset)
   session.add(detrended_dataset)
   session.commit()

   # Query the hierarchy
   print(f"Raw dataset has {len(raw_dataset.derived_datasets)} derived products")
   print(f"Detrended dataset has {len(detrended_dataset.source_datasets)} sources")

Database Constraints
--------------------

The schema enforces several important constraints:

1. **Unique Constraints**:

   - Mission.name must be unique
   - MissionCatalog.name must be unique
   - Target: (catalog_id, name) combination must be unique
   - PhotometricSource.name must be unique
   - ProcessingMethod.name must be unique
   - FITSFrame: (type, cadence) combination must be unique
   - QualityFlagArray: (type, observation_id, target_id) must be unique (with NULL handling)

2. **Cascade Deletes**:

   - Deleting an Observation cascades to FITSFrame, TargetSpecificTime, DataSet, and QualityFlagArray
   - Deleting a PhotometricSource or ProcessingMethod cascades to DataSet
   - Deleting a Target cascades to DataSet
   - Deleting a DataSet cascades to DataSetHierarchy entries

3. **Referential Integrity**:

   - All foreign keys are enforced at the database level
   - Orphaned records are prevented through proper relationships
   - DataSetHierarchy maintains referential integrity for lineage tracking
