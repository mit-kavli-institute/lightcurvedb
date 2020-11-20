from hypothesis import strategies as st, given, settings, note
from lightcurvedb.models import Orbit
from .factories import orbit_frames
from .fixtures import db_conn, clear_all


@settings(deadline=None)
@given(orbit_frames())
def test_min_max_cadence_retrieval(db_conn, orbit_frames):
    with db_conn as db:
        try:
            orbit = orbit_frames
            frames = orbit_frames.frames
            db.add(orbit)
            db.add(frames[0].frame_type)
            db.commit()

            db.session.add_all(frames)
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
    with db_conn as db:
        try:
            # Add prerequisites
            orbit = orbit_frames
            frames = orbit_frames.frames
            db.add(orbit)
            db.commit()

            db.add(frames[0].frame_type)
            db.commit()

            orbit.frames = frames
            db.session.add_all(frames)
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
