from hypothesis import strategies as st
from hypothesis import given, note, assume
from lightcurvedb.models.lightcurve import Lightcurve, Lightpoint

import numpy as np
from .fixtures import db_conn
from .factories import lightcurve as lightcurve_st


@given(lightcurve_st())
def test_subscriptable(lightcurve):
    np.testing.assert_array_equal(lightcurve['cadences'], lightcurve.cadences)
    np.testing.assert_array_equal(lightcurve['bjd'], lightcurve.bjd)
    np.testing.assert_array_equal(lightcurve['values'], lightcurve.values)
    np.testing.assert_array_equal(lightcurve['errors'], lightcurve.errors)
    np.testing.assert_array_equal(lightcurve['x_centroids'], lightcurve.x_centroids)
    np.testing.assert_array_equal(lightcurve['y_centroids'], lightcurve.y_centroids)
    np.testing.assert_array_equal(lightcurve['quality_flags'], lightcurve.quality_flags)

@given(lightcurve_st())
def test_subscriptable_set(lightcurve):

    check = [1,2,3,4]

    lightcurve['cadences'] = check
    lightcurve['bjd'] = check
    lightcurve['values'] = check
    lightcurve['errors'] = check
    lightcurve['x_centroids'] = check
    lightcurve['y_centroids'] = check
    lightcurve['quality_flags'] = check


    np.testing.assert_array_equal(lightcurve['cadences'], check)
    np.testing.assert_array_equal(lightcurve['bjd'], check)
    np.testing.assert_array_equal(lightcurve['values'], check)
    np.testing.assert_array_equal(lightcurve['errors'], check)
    np.testing.assert_array_equal(lightcurve['x_centroids'], check)
    np.testing.assert_array_equal(lightcurve['y_centroids'], check)
    np.testing.assert_array_equal(lightcurve['quality_flags'], check)

