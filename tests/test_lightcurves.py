from hypothesis import strategies as st
from hypothesis import given, note
from lightcurvedb.models.lightcurve import Lightcurve

from .fixtures import db_conn
from .factories import lightcurve as lightcurve_st

@given(lightcurve_st())
def test_orbit_lightcurve_instantiation(lightcurve):
    length = len(lightcurve)
    attrs = [
        lightcurve.cadences,
        lightcurve.bjd,
        lightcurve.flux,
        lightcurve.flux_err,
        lightcurve.x_centroids,
        lightcurve.y_centroids,
        lightcurve.quality_flags,
    ]

    for attr in attrs:
        note(attr)

    assert all(len(attr) == length for attr in attrs)
