LightcurveDB Manager
====================

LightcurveManagers allow for bulk interaction of many lightcurves without
the need to hit the database multiple times. This class allows you to make
one bulk query and interface the collection in a dictionary-like manner.

.. code-block:: python

    from lightcurvedb import db, Lightcurve, LightcurveManager

    q = db.lightcurves.filter(Lightcurve.tic_id % 2 == 0)

    # LightcurveManager accepts any iterable of lightcurves
    lm = LightcurveManager(q)

    # You can iterate over lightcurve managers
    for lc in lm:
        print(lc)
        # <Lightcurve KSPMagnitude 1230 Aperture_002>
        # <Lightcurve KSPMagnitude 1230 Aperture_003>
        # <Lightcurve RawMagnitude 1230 Aperture_001>
        # <Lightcurve KSPMagnitude 6420 Aperture_004>
        # ...etc

    # You can filter for any attribute
    for lc in lm['KSPMagnitude']:
        print(lc)
        # <Lightcurve KSPMagnitude 1230 Aperture_002>
        # <Lightcurve KSPMagnitude 1230 Aperture_003>
        # <Lightcurve KSPMagnitude 1230 Aperture_001>
        # <Lightcurve KSPMagnitude 6420 Aperture_004>
        # ...etc

    # You can chain filters
    for lc in lm['KSPMagnitude']['Aperture_001']:
        print(lc)
        # <Lightcurve KSPMagnitude 1230 Aperture_001>
        # <Lightcurve KSPMagnitude 6420 Aperture_001>
        # <Lightcurve KSPMagnitude 5456 Aperture_001>
        # <Lightcurve KSPMagnitude 8900 Aperture_001>
        # ...etc

    # Or you can get a single lightcurve
    lc = lm['KSPMagnitude']['Aperture_004'][1230]
    print(lc)  # <Lightcurve KSPMagnitude 1230 Aperture_004>

    # Fields can be specified in any order
    lc = lm[1230]['KSPMagnitude']['Aperture_001']

    # Singular lightcurves are returned as soon as a filter
    # would return a single lightcurve.
    lm = LightcurveManager(
        db.lightcurves(Lightcurve.tic_id == 1230)
    )

    lm[1230]  # tautological, the lightcurve view does not change

    # but
    lc = lm['KSPMagnitude']['Aperture_002']  # returns single lc

    # Keep this in mind when reducing queries.


.. autoclass:: lightcurvedb.managers.lightcurve_query.LightcurveManager
   :members:
