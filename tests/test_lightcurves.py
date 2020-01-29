from hypothesis import strategies as st
from hypothesis import given, note
from lightcurvedb.models.lightcurve import OrbitLightcurve

from .fixtures import db_conn
from .factories import orbit_lightcurve as orbit_lightcurve_st

@given(orbit_lightcurve_st())
def test_orbit_lightcurve_instantiation(orbit_lightcurve):
    length = len(orbit_lightcurve)
    attrs = [
        orbit_lightcurve.cadences,
        orbit_lightcurve.bjd,
        orbit_lightcurve.flux,
        orbit_lightcurve.flux_err,
        orbit_lightcurve.x_centroids,
        orbit_lightcurve.y_centroids,
        orbit_lightcurve.meta
    ]

    for attr in attrs:
        note(attr)

    assert all(len(attr) == length for attr in attrs)