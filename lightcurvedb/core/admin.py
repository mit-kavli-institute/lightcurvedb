"""
This module creates admin level POSTGRESQL attributes
"""
from sqlalchemy import MetaData

def reflect_psql_admin(engine):
    PSQL_META = MetaData()
    
    PSQL_META.reflect(
        engine, schema='pg_catalog'   
    )

    return PSQL_META


def psql_tables(lcdb):
    return reflect_psql_admin(lcdb.session.engine)
