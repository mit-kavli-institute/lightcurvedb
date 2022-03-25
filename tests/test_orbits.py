from hypothesis import given, settings, HealthCheck
from lightcurvedb.models import Orbit
from .conftest import db
from .strategies import orm

# We rollback all changes to remote db
no_scope_check = HealthCheck.function_scoped_fixture

@settings(suppress_health_check=[no_scope_check])
@given(orm.orbits())
def test_insert_orbit(db, orbit):
    db.add(orbit)
    db.flush()

    assert db.query(Orbit).filter_by(id=orbit.id).count() == 1
    assert db.query(Orbit).filter_by(orbit_number=orbit.orbit_number).count() == 1

    db.rollback()
