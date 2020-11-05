from hypothesis import strategies as st, given, settings, note
from lightcurvedb.models import Orbit
from .factories import orbit_frames
from .fixtures import db_conn, clear_all


@settings(deadline=None)
@given(orbit_frames())
def test_min_max_cadence_retrieval(db_conn, orbit_frames):
    with db_conn as db:
        try:
            # Add prerequisites
            orbit = orbit_frame
            frames = orbit_frame.frames
            db.add(orbit)
            db.add(frames[0])
            db.commit()

            note("orbit id: {0}".format(orbit.id))
            for f in frames:
                f.orbit_id = orbit.id

            db.session.add_all(orbit.frames)
            db.commit()

            ref_min_cadence = orbit.min_cadence
            ref_max_cadence = orbit.max_cadence

            check_min_cadence = db.query(
                Orbit.min_cadence
            ).first()[0]
            check_max_cadence = db.query(
                Orbit.max_cadence
            ).first()[0]

            assert check_min_cadence == ref_min_cadence
            assert check_max_cadence == ref_max_cadence
        finally:
            db.rollback()
            clear_all(db)


@settings(deadline=None)
@given(orbit_frames())
def test_min_max_gps_time_retrieval(db_conn, orbit_frames):
    orbit = orbit_frames
    with db_conn as db:
        try:
            # Add prerequisites
            db.add(orbit)
            db.add(orbit.frames[0].frame_type)
            db.commit()

            note("orbit id: {0}".format(orbit.id))
            orbit = db.orbits.get(orbit.id)
            for f in orbit.frames:
                f.orbit_id = orbit.id

            db.session.add_all(orbit.frames)
            db.commit()

            sorted_frames = list(sorted(orbit.frames, key=lambda f: f.cadence))

            ref_min_gps = sorted_frames[0].gps_time
            ref_max_gps = sorted_frames[-1].gps_time

            check_min_gps = db.query(
                Orbit.min_gps_time
            ).first()[0]
            check_max_gps = db.query(
                Orbit.max_gps_time
            ).first()[0]

            assert check_min_gps == ref_min_gps
            assert check_max_gps == ref_max_gps
        finally:
            db.rollback()
            clear_all(db)
