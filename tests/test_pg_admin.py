from lightcurvedb.core.admin import psql_tables

from .fixtures import db_conn


def test_can_get_psql_admin(db_conn):
    with db_conn as db:
        psql = psql_tables(db)
        assert psql
