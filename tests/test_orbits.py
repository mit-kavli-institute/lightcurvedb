from hypothesis import strategies as st
from hypothesis import given

from lightcurvedb.models.orbit import Orbit

from .fixtures import db_conn
from .factories import orbit as orbit_st

@given(orbit_st())
def test_orbit_retrieval(db_conn, orbit):
    db_conn.session.begin_nested()
    db_conn.add(orbit)
    db_conn.commit()
    q = db_conn.session.query(Orbit).get(orbit.id)
    assert q is not None
    db_conn.session.rollback()