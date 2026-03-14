API Reference
=============

This page lists all public classes, functions, and constants available to
developers using LightcurveDB. Internal (underscore-prefixed) members are
omitted.

.. contents:: Sections
   :local:
   :depth: 1

Top-Level Exports
-----------------

These are available directly from ``import lightcurvedb``:

.. autodata:: lightcurvedb.__version__
   :no-value:
   :no-index:

.. autofunction:: lightcurvedb.db_from_config
   :no-index:

.. autodata:: lightcurvedb.db
   :no-index:

.. autodata:: lightcurvedb.LCDB_Session
   :no-index:

Models
------

All models are importable from ``lightcurvedb.models``.

Mission & Catalog
~~~~~~~~~~~~~~~~~

.. autoclass:: lightcurvedb.models.Mission
   :members:
   :exclude-members: metadata, registry
   :show-inheritance:
   :no-index:

.. autoclass:: lightcurvedb.models.MissionCatalog
   :members:
   :exclude-members: metadata, registry
   :show-inheritance:
   :no-index:

.. autoclass:: lightcurvedb.models.Target
   :members:
   :exclude-members: metadata, registry
   :show-inheritance:
   :no-index:

Instrument
~~~~~~~~~~

.. autoclass:: lightcurvedb.models.Instrument
   :members:
   :exclude-members: metadata, registry
   :show-inheritance:
   :no-index:

Observation & Time
~~~~~~~~~~~~~~~~~~

.. autoclass:: lightcurvedb.models.Observation
   :members:
   :exclude-members: metadata, registry
   :show-inheritance:
   :no-index:

.. autoclass:: lightcurvedb.models.TargetSpecificTime
   :members:
   :exclude-members: metadata, registry
   :show-inheritance:
   :no-index:

Frames
~~~~~~

.. autoclass:: lightcurvedb.models.FITSFrame
   :members:
   :exclude-members: metadata, registry
   :show-inheritance:
   :no-index:

Processing
~~~~~~~~~~

.. autoclass:: lightcurvedb.models.PhotometricSource
   :members:
   :exclude-members: metadata, registry, name
   :show-inheritance:
   :no-index:

.. autoclass:: lightcurvedb.models.ProcessingMethod
   :members:
   :exclude-members: metadata, registry, name
   :show-inheritance:
   :no-index:

Data Products
~~~~~~~~~~~~~

.. autoclass:: lightcurvedb.models.DataSet
   :members:
   :exclude-members: metadata, registry
   :show-inheritance:
   :no-index:

.. autoclass:: lightcurvedb.models.DataSetHierarchy
   :members:
   :exclude-members: metadata, registry
   :show-inheritance:
   :no-index:

Quality Flags
~~~~~~~~~~~~~

.. autoclass:: lightcurvedb.models.QualityFlagArray
   :members:
   :exclude-members: metadata, registry, created_on
   :show-inheritance:
   :no-index:

Base Class & Mixins
-------------------

These are importable from ``lightcurvedb.core.base_model`` and are useful
when extending the ORM.

.. autoclass:: lightcurvedb.core.base_model.LCDBModel
   :members: __repr__, __rich_repr__, __rich_console__
   :show-inheritance:
   :no-index:

.. autoclass:: lightcurvedb.core.base_model.CreatedOnMixin
   :members:
   :no-index:

.. autoclass:: lightcurvedb.core.base_model.NameAndDescriptionMixin
   :members:
   :no-index:

Custom Types
~~~~~~~~~~~~

.. autoclass:: lightcurvedb.core.types.NumpyArrayType
   :members: process_result_value, process_bind_param, coerce_compared_value
   :show-inheritance:
   :no-index:

Connection & Session Management
-------------------------------

.. autofunction:: lightcurvedb.core.connection.db_from_config
   :no-index:

.. autofunction:: lightcurvedb.core.connection.configure_engine
   :no-index:

.. autodata:: lightcurvedb.core.connection.LCDB_Session
   :no-index:

I/O & Pipeline
--------------

.. autofunction:: lightcurvedb.io.db_scope
   :no-index:

Utilities
---------

Iteration Helpers
~~~~~~~~~~~~~~~~~

.. autofunction:: lightcurvedb.util.iter.chunkify
   :no-index:

.. autofunction:: lightcurvedb.util.iter.eq_partitions
   :no-index:

Path Context Extraction
~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: lightcurvedb.util.contexts.REGISTER
   :no-index:

.. autofunction:: lightcurvedb.util.contexts.extract_pdo_path_context
   :no-index:

Constants
~~~~~~~~~

.. autodata:: lightcurvedb.util.constants.DEFAULT_CONFIG_PATH
   :no-index:
