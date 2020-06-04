import pytest
from hypothesis import given, note
from hypothesis import strategies as st
from lightcurvedb.legacy.query import QlpQuery
from .factories import orbit as orbit_st, frame as frame_st
from .fixtures import db_conn, clear_all


@given(orbit_st())
def test_get_orbits(db_conn, orbit):
    try:
        db_conn.session.begin_nested()
        db_conn.add(orbit)
        db_conn.commit()

        q = QlpQuery(conn=db_conn)

        orbit_number = orbit.orbit_number

        result = q.query_orbits_by_id([orbit_number])
        assert result is not None

        db_conn.session.rollback()
    except Exception:
        db_conn.session.rollback()
        raise
    finally:
        for remove in clear_all():
            db_conn.session.execute(remove)
        db_conn.commit()

