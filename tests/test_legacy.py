import pytest
from hypothesis import given, note
from hypothesis import strategies as st
from lightcurvedb.legacy import QlpQuery
from lightcurvedb.models import Frame
from sqlalchemy.sql.expression import func
from .factories import orbit as orbit_st, frame as frame_st, frame_type as frametype_st, orbit_frames
from .fixtures import db_conn, clear_all, near_equal


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


@given(orbit_frames())
def test_orbit_cadence_limit(db_conn, frames):
    try:
        orbit = frames[0].orbit

        note(frames)
        db_conn.session.begin_nested()
        db_conn.add(frames[0].orbit)
        db_conn.session.add_all(frames)
        db_conn.commit()
        q = QlpQuery(conn=db_conn)
        min_c, max_c = q.query_orbit_cadence_limit(orbit.orbit_number, 30, 1)

        assert near_equal(min_c, min([f.cadence for f in frames]))
        assert near_equal(max_c, max([f.cadence for f in frames]))

        db_conn.session.rollback()
    except Exception:
        db_conn.session.rollback()
        for remove in clear_all():
            db_conn.session.execute(remove)
        db_conn.commit()

        raise
    finally:
        for remove in clear_all():
            db_conn.session.execute(remove)
        db_conn.commit()


@given(orbit_frames())
def test_orbit_tjd_limit(db_conn, frames):
    try:
        orbit = frames[0].orbit

        db_conn.session.begin_nested()
        db_conn.add(frames[0].orbit)
        db_conn.session.add_all(frames)
        db_conn.commit()

        q = QlpQuery(conn=db_conn)
        min_tjd, max_tjd = q.query_orbit_tjd_limit(orbit.orbit_number, 30, 1)

        assert near_equal(min_tjd, min(f.start_tjd for f in frames))
        assert near_equal(max_tjd, max(f.end_tjd for f in frames))
        db_conn.session.rollback()

    except Exception:
        db_conn.session.rollback()
        for remove in clear_all():
            db_conn.session.execute(remove)
        db_conn.commit()
        raise
    finally:
        for remove in clear_all():
            db_conn.session.execute(remove)
        db_conn.commit()


@given(orbit_frames())
def test_get_frames_by_orbit(db_conn, frames):
    try:
        orbit = frames[0].orbit
        db_conn.session.begin_nested()
        db_conn.add(frames[0].orbit)
        db_conn.session.add_all(frames)
        db_conn.commit()

        q = QlpQuery(conn=db_conn)
        frames_result = q.query_frames_by_orbit(orbit.orbit_number, 30, 1)
        note(frames_result)
        assert all(orbit.orbit_number == f[0] for f in frames_result)

        db_conn.session.rollback()

    except Exception:
        db_conn.session.rollback()
        for remove in clear_all():
            db_conn.session.execute(remove)
        db_conn.commit()
        raise
    finally:
        for remove in clear_all():
            db_conn.session.execute(remove)
        db_conn.commit()
