"""
This module creates admin level POSTGRESQL attributes
"""
from sqlalchemy import MetaData

from lightcurvedb.util.decorators import suppress_warnings

PSQL_META = None


def reflect_psql_admin(engine):
    global PSQL_META

    if PSQL_META:
        # Cowardly no-op
        return PSQL_META

    PSQL_META = MetaData()

    PSQL_META.reflect(engine, schema="pg_catalog")

    return PSQL_META


@suppress_warnings
def psql_tables(lcdb):
    # PSQL typedefs a bunch of columns which SQLAlchemy complains about
    # (but still perfectly utilizes)
    return reflect_psql_admin(lcdb.session.get_bind())


def psql_catalog_tables():
    try:
        return PSQL_META.tables
    except AttributeError:
        raise RuntimeError(
            "Looks like PSQL_META has not been initialized. "
            "Please call `psql_tables(db_instance)`."
        )


def get_psql_catalog_tables(*tables):
    catalogs = psql_catalog_tables()
    try:
        results = tuple(
            catalogs["pg_catalog.{0}".format(table)] for table in tables
        )
    except KeyError:
        raise KeyError(
            "Unknown catalogs {0}. Registered catalogs: {1}".format(
                *tables, catalogs.keys()
            )
        )
    if len(results) == 1:
        return results[0]
    return results
