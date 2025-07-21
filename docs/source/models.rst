Models
======

The lightcurvedb models represent the core data structures for storing and retrieving astronomical time-series data from the TESS mission.

Frame Models
------------

.. autoclass:: lightcurvedb.models.FITSFrame
   :members:
   :show-inheritance:

Instrument Models
-----------------

.. autoclass:: lightcurvedb.models.Instrument
   :members:
   :show-inheritance:

Processing Models
-----------------

.. autoclass:: lightcurvedb.models.PhotometricSource
   :members:
   :show-inheritance:
   :exclude-members: name

.. autoclass:: lightcurvedb.models.DetrendingMethod
   :members:
   :show-inheritance:
   :exclude-members: name

.. autoclass:: lightcurvedb.models.ProcessingGroup
   :members:
   :show-inheritance:
   :exclude-members: name

.. autoclass:: lightcurvedb.models.DataSet
   :members:
   :show-inheritance:

Observation Models
------------------

.. autoclass:: lightcurvedb.models.Observation
   :members:
   :show-inheritance:

.. autoclass:: lightcurvedb.models.TargetSpecificTime
   :members:
   :show-inheritance:

Quality Flag Models
-------------------

.. autoclass:: lightcurvedb.models.QualityFlagArray
   :members:
   :show-inheritance:
   :exclude-members: created_on

Target Models
-------------

.. autoclass:: lightcurvedb.models.Mission
   :members:
   :show-inheritance:

.. autoclass:: lightcurvedb.models.MissionCatalog
   :members:
   :show-inheritance:

.. autoclass:: lightcurvedb.models.Target
   :members:
   :show-inheritance:
