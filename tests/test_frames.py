from hypothesis import given, settings, HealthCheck, assume
from lightcurvedb.models import Orbit, FrameType, Frame
from .conftest import db
from .strategies import orm

# We rollback all changes to remote db
no_scope_check = HealthCheck.function_scoped_fixture

@settings(suppress_health_check=[no_scope_check])
@given(orm.frame_types(), orm.orbits(), orm.frames())
def test_frame_insertion(db, frame_type, orbit, frame):
    db.add(orbit)
    db.add(frame_type)
    db.flush()
    frame.frame_type = frame_type
    frame.orbit = orbit
    db.add(frame)

    # Tautological query of frame
    db.query(Frame).filter_by(id=frame.id).count() == 1

    # Obtain frame by type
    db.query(Frame).filter(Frame.frame_type == frame_type).count() == 1

    # Obtain frame by orbit
    db.query(Frame).filter(Frame.orbit == orbit).count() == 1

    db.rollback()
