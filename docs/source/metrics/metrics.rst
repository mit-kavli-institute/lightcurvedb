Performing LightcurveDB Metrics
===============================
For long term performance checks over time, lightcurvedb has models to help
store and describe operations done. This document will describe will help
guide where to apply these models and how to interact, create, destroy, and
retrieve them.


The following models are defined in the Metrics module. ``QLPStage``,
``QLPProcess`` and ``QLPOperation``, the former being the base abstract class
to differentiate them from the other Models defined in lightcurvedb.


``QLPProcess`` should be used to describe the overall operation occuring.
This might be a specific model ingestion or a bulk alteration being performed.
This model also provides versioning support to allow comparisons between
different versions of the same operation.


``QLPAlteration`` should be used to describe individual jobs being performed.
These jobs can have the attributes of job size and the number of items
interacted with along with the interaction type. For example an ingestion of
Lightcurves will perform a ``'INSERT`'`` operation on `n` items.

More importantly ``QLPAlteration`` will contain the start and end times it
took to complete the operation. These times are stored as UTC datetime
objects.

Classes
#######
.. autoclass:: lightcurvedb.models.QLPStage
    :members:

.. autoclass:: lightcurvedb.models.metrics.QLPProcess
    :members:

.. autoclass:: lightcurvedb.models.metrics.QLPOperation
    :members:
