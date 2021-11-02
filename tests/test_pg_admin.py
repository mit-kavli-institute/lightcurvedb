from lightcurvedb.core.admin import get_psql_catalog_tables, psql_tables

from .fixtures import db_conn  # noqa F401


def test_can_get_psql_admin(db_conn):  # noqa F401
    with db_conn as db:
        psql = psql_tables(db)
        assert psql

        pg_class = get_psql_catalog_tables("pg_class")
        assert pg_class.name == "pg_class"
