from hypothesis import strategies as st
from hypothesis import given, note
from lightcurvedb.models.lightcurve import Lightcurve

from .fixtures import db_conn
from .factories import lightcurve as lightcurve_st

@given(lightcurve_st())
def test_orbit_lightcurve_instantiation(db_conn, lightcurve):
    db_conn.session.begin_nested()
    db_conn.add(lightcurve)
    note(lightcurve)
    length = len(lightcurve)
    attrs = [
        lightcurve.cadences,
        lightcurve.bjd,
        lightcurve.values,
        lightcurve.errors,
        lightcurve.x_centroids,
        lightcurve.y_centroids,
        lightcurve.quality_flags,
    ]

    for attr in attrs:
        note(attr)

    assert all(len(attr) == length for attr in attrs)

    db_conn.session.rollback()
