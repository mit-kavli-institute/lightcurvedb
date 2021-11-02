from sqlalchemy import text

from lightcurvedb.core.admin import psql_tables
from lightcurvedb.models import Lightcurve, Orbit

from .fixtures import clear_all, db_conn


def test_get_oid(db_conn):
    with db_conn as db:
        psql_tables(db)
        assert db.query(Lightcurve.oid) is not None


def test_get_orbit_properties():
    property, contexts = Orbit.get_property("orbit_number")
    assert not "join_contexts" in contexts
