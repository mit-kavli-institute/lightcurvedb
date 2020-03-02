from hypothesis import strategies as st
from hypothesis import given, note

from lightcurvedb.models.orbit import Orbit

from .fixtures import db_conn
from .factories import orbit as orbit_st

@given(orbit_st())
def test_orbit_retrieval(db_conn, orbit):
    db_conn.session.begin_nested()
    try:
        db_conn.add(orbit)
        db_conn.commit()
        q = db_conn.orbits.get(orbit.id)
        assert q is not None
        db_conn.session.rollback()
    except:
        db_conn.session.rollback()
        note(orbit)
        raise
