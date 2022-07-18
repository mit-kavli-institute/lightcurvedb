from hypothesis import given, settings, HealthCheck, assume
from lightcurvedb.models import Orbit
from .strategies import orm

# We rollback all changes to remote db
no_scope_check = HealthCheck.function_scoped_fixture

@settings(suppress_health_check=[no_scope_check])
@given(orm.orbits())
def test_insert_orbit(db, orbit):
    defined_ids = set(id_ for id_, in db.query(Orbit.id))
    assume(orbit.id not in defined_ids)  # avoid clobbering other tests

    db.add(orbit)
    db.flush()

    assert db.query(Orbit).filter_by(orbit_number=orbit.orbit_number).count() == 1

    db.rollback()
