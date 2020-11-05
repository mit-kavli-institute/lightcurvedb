Lightcurve
==========
Lightcurves in lightcurvedb are defined as an SQLAlchemy model which allow
users to discern independent stellar data (lightpoints). A unique lightcurve
is defined as a unique set of 3 attributes:

* TIC ID

  * A large integer representing the parent star which is the source of the
    lightcurve.
* Aperture

  * The photometric aperture used to extract measurements on the parent star.
* Lightcurve Type

  * A logical discriminator which allows discernment of various types of
    Lightcurves. Such as those which have been detrended through various
    algorithms. That context is retained in this field.

If a user can provide these 3 attributes then it is guaranteed to reduce down
to a single lightcurve or result in 0 rows.


Accessing the internal stellar timeseries data is straightforward. There are
several data attributes which can be accessed on a single lightcurve.

* Cadence

  * An integer representing the discrete nth exposure. This value increases
    by with each subsequent exposure.
  * .. note::
    There is a cadence jump in the first extended mission cadence from TESS.
    This cadence numbering change reflects the exposure time change from
    30 minutes to 10 minutes.
* Barycentric Julian Date

  * The time that each exposure was recorded. This time has
    been corrected for Earth-centric observations.
  * Each value is interpreted as the number of days since epoch 2457000.
* Values

  * The stellar brightness that has been extracted from the exposure using
    the related aperture. This value's units depend on the lightcurve type.
    It can be in flux or magnitude values. (See the related
    ``LightcurveType.description`` attribute to know how to interpret
    units).
  * This field also has aliases for easier QLP integration, but they can
    also be used by you.

    * flux
    * mag
    * data
* Errors

  * The error of each value.
  * This field has aliases for easier QLP integration, but they can also be
    used by you.

    * flux_err
    * mag_err
* X Centroids

  * The X-pixel coordinate of the related aperture.
* Y Centroids

  * The Y-pixel coordinate of the related aperture.
* Quality Flags

  * The quality flag bit assignment.
    * 0th bit: Set to 0 for 'OK' set to 1 for 'BAD'.

Data Assignment.
Assigning data within a lightcurve is pretty straightforward.

.. code-block:: python
    :linenos:

    from lightcurvedb import db, Lightcurve

    # assume we get some _single_ lightcurve
    lc = db.lightcurves.filter_by(
        tic_id=123456789,
        aperture_id='Aperture_003',
        lightcurve_type_id='RawMagnitude'
    ).one()

    # Let's say we want to assign updated values to the lightcurve.
    # let's also assume that the 'length' of this lightcurve (number of
    # exposures) is 3.
    print(lc['mag'])  # [12.34, 12.37. 12.31]
    updated_values = [0.0, 1.0, 2.0]

    lc['mag'] = updated_values
    print(lc['mag'])  # [0.0, 1.0, 2.0]

    # These changes are not reflected in the database unless...
    db.commit()

.. warning::
    If the assigned values are shorter or longer than the lightcurve then a
    ValueError is raised.

Cadence Keying

Sometimes we want to index lightcurves by some set of cadences; such as when
we want to mask out a single orbit. This can be done through cadence keying
which is an interface exposed through ``lightcurve.lightpoints``.

.. code-block:: python
    :linenos:

    from lightcurvedb import db, Lightcurve
    # assume we get some _single_ lightcurve
    lc = db.lightcurves.filter_by(
        tic_id=123456789,
        aperture_id='Aperture_003',
        lightcurve_type_id='RawMagnitude'
    ).one()

    print(lc['cadences'])  # [1, 2, 10, 20, 100, 200]
    print(lc['mag'])  # [1.0, 2.0, 10.0, 20.0, 100.0, 200.0]

    # Let's assume orbit 1 has cadences 1 and 2
    idx = [1, 2]
    sliced = lc.lightpoints[idx]
    print(sliced['mag'])  # [1.0, 2.0]

    # Order is respected
    idx = [2, 1]
    print(lc.lightpoints[idx]['mag'])  # [2.0, 1.0]

    # Missing cadences are omitted
    idx = [1, 2, 3, 10]
    print(lc.lightpoints[idx]['mag'])  # [1.0, 2.0, 10.0]

    # Slices can also be assigned values

    idx = [10, 20]
    new_values = [0.10, 0.20]
    lc.lightpoints[idx]['mag'] = new_values  # is OK since len(slice) == 2
    print(lc['mag'])  # [1.0, 2.0, 0.10, 0.20, 100.0, 200.0]

    # Changes are reflected in the database unless
    db.commit()


.. autoclass:: lightcurvedb.models.lightcurve.Lightcurve
   :members:
