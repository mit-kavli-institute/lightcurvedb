from hypothesis import strategies as st, given, settings, note
from lightcurvedb.models import Orbit
from .strategies import frame_types, orbits, frames, orbit_frames
from .fixtures import db_conn, clear_all


@settings(deadline=None)
@given(st.data())
def test_min_max_cadence_retrieval(db_conn, data):
    with db_conn as db:

        try:
            frame_type = data.draw(frame_types())
            orbit = data.draw(orbits())

            db.add(orbit)
            db.add(frame_type)
            db.commit()

            note("orbit-id {0}".format(orbit.id))
            frame_list = orbit_frames(data, orbit, frame_type)

            db.session.add_all(frame_list)
            db.commit()

            orbit = db.orbits.get(orbit.id)

            ref_min_cadence = orbit.min_cadence
            ref_max_cadence = orbit.max_cadence

            check_min_cadence = db.query(Orbit.min_cadence).first()[0]
            check_max_cadence = db.query(Orbit.max_cadence).first()[0]

            assert check_min_cadence == ref_min_cadence
            assert check_max_cadence == ref_max_cadence
        finally:
            db.rollback()
            clear_all(db)


@settings(deadline=None)
@given(st.data())
def test_min_max_gps_time_retrieval(db_conn, data):
    with db_conn as db:
        try:
            frame_type = data.draw(frame_types())
            orbit = data.draw(orbits())

            db.add(orbit)
            db.add(frame_type)
            db.commit()

            note("orbit-id {0}".format(orbit.id))
            note("frame_type {0}".format(frame_type.name))
            frame_list = orbit_frames(data, orbit, frame_type)

            db.session.add_all(frame_list)
            db.commit()
            orbit = db.orbits.get(orbit.id)

            sorted_frames = list(sorted(frame_list, key=lambda f: f.cadence))

            ref_min_gps = sorted_frames[0].gps_time
            ref_max_gps = sorted_frames[-1].gps_time

            check_min_gps = db.query(Orbit.min_gps_time).first()[0]
            check_max_gps = db.query(Orbit.max_gps_time).first()[0]

            assert check_min_gps == ref_min_gps
            assert check_max_gps == ref_max_gps
        finally:
            db.rollback()
            clear_all(db)
