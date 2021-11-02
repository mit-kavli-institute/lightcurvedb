from lightcurvedb.core.admin import psql_tables
from lightcurvedb.models import Lightcurve, Orbit

from .fixtures import clear_all, db_conn  # noqa F401


def test_get_oid(db_conn):  # noqa F401
    with db_conn as db:
        psql_tables(db)
        assert db.query(Lightcurve.oid) is not None


def test_get_orbit_properties():
    property, contexts = Orbit.get_property("orbit_number")
    assert "join_contexts" not in contexts
