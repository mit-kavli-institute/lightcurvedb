"""
This module creates admin level POSTGRESQL attributes
"""
from sqlalchemy import MetaData
from lightcurvedb.util.decorators import suppress_warnings


def reflect_psql_admin(engine):
    PSQL_META = MetaData()

    PSQL_META.reflect(
        engine, schema='pg_catalog'
    )

    return PSQL_META


@suppress_warnings
def psql_tables(lcdb):
    # PSQL typedefs a bunch of columns which SQLAlchemy complains about
    # (but still perfectly utilizes)
    return reflect_psql_admin(lcdb.session.get_bind())
