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

.. autofunction:: lightcurvedb.db_from_config

.. autodata:: lightcurvedb.db

.. autodata:: lightcurvedb.LCDB_Session

Models
------

All models are importable from ``lightcurvedb.models``.

Mission & Catalog
~~~~~~~~~~~~~~~~~

.. autoclass:: lightcurvedb.models.Mission
   :members:
   :exclude-members: metadata, registry
   :show-inheritance:

.. autoclass:: lightcurvedb.models.MissionCatalog
   :members:
   :exclude-members: metadata, registry
   :show-inheritance:

.. autoclass:: lightcurvedb.models.Target
   :members:
   :exclude-members: metadata, registry
   :show-inheritance:

Instrument
~~~~~~~~~~

.. autoclass:: lightcurvedb.models.Instrument
   :members:
   :exclude-members: metadata, registry
   :show-inheritance:

Observation & Time
~~~~~~~~~~~~~~~~~~

.. autoclass:: lightcurvedb.models.Observation
   :members:
   :exclude-members: metadata, registry
   :show-inheritance:

.. autoclass:: lightcurvedb.models.TargetSpecificTime
   :members:
   :exclude-members: metadata, registry
   :show-inheritance:

Frames
~~~~~~

.. autoclass:: lightcurvedb.models.FITSFrame
   :members:
   :exclude-members: metadata, registry
   :show-inheritance:

Processing
~~~~~~~~~~

.. autoclass:: lightcurvedb.models.PhotometricSource
   :members:
   :exclude-members: metadata, registry, name
   :show-inheritance:

.. autoclass:: lightcurvedb.models.ProcessingMethod
   :members:
   :exclude-members: metadata, registry, name
   :show-inheritance:

Data Products
~~~~~~~~~~~~~

.. autoclass:: lightcurvedb.models.DataSet
   :members:
   :exclude-members: metadata, registry
   :show-inheritance:

.. autoclass:: lightcurvedb.models.DataSetHierarchy
   :members:
   :exclude-members: metadata, registry
   :show-inheritance:

Quality Flags
~~~~~~~~~~~~~

.. autoclass:: lightcurvedb.models.QualityFlagArray
   :members:
   :exclude-members: metadata, registry, created_on
   :show-inheritance:

Base Class & Mixins
-------------------

These are importable from ``lightcurvedb.core.base_model`` and are useful
when extending the ORM.

.. autoclass:: lightcurvedb.core.base_model.LCDBModel
   :members: __repr__, __rich_repr__, __rich_console__
   :show-inheritance:

.. autoclass:: lightcurvedb.core.base_model.CreatedOnMixin
   :members:

.. autoclass:: lightcurvedb.core.base_model.NameAndDescriptionMixin
   :members:

Custom Types
~~~~~~~~~~~~

.. autoclass:: lightcurvedb.core.types.NumpyArrayType
   :members: process_result_value, process_bind_param, coerce_compared_value
   :show-inheritance:

Connection & Session Management
-------------------------------

.. autofunction:: lightcurvedb.core.connection.db_from_config

.. autofunction:: lightcurvedb.core.connection.configure_engine

.. autodata:: lightcurvedb.core.connection.LCDB_Session

I/O & Pipeline
--------------

.. autofunction:: lightcurvedb.io.db_scope

Utilities
---------

Iteration Helpers
~~~~~~~~~~~~~~~~~

.. autofunction:: lightcurvedb.util.iter.chunkify

.. autofunction:: lightcurvedb.util.iter.eq_partitions

Path Context Extraction
~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: lightcurvedb.util.contexts.REGISTER

.. autofunction:: lightcurvedb.util.contexts.extract_pdo_path_context

Constants
~~~~~~~~~

.. autodata:: lightcurvedb.util.constants.DEFAULT_CONFIG_PATH
