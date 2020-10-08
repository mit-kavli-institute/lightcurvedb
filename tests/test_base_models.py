from lightcurvedb.core.admin import psql_tables
from lightcurvedb.models import Lightcurve, Orbit
from sqlalchemy import text
from .fixtures import db_conn, clear_all


def test_get_oid(db_conn):
    with db_conn as db:
        psql_tables(db)
        assert db.query(Lightcurve.oid) is not None
