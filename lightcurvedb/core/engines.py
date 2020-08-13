from __future__ import division, print_function

import warnings
import os

try:
    from configparser import ConfigParser
except ImportError:
    # Python 2?
    from ConfigParser import ConfigParser

from sqlalchemy import create_engine
from sqlalchemy.event import listens_for
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import scoped_session, sessionmaker, create_session
from sqlalchemy.exc import DisconnectionError


DB_TYPE = 'postgresql+psycopg2'
DEFAULT_ENGINE_KWARGS = dict(
    pool_size=16,
    max_overflow=-1,
    executemany_mode='values',
    executemany_values_page_size=10000
)

def __config_to_url__(path):
    parser = ConfigParser()
    parser.read(path)
    kwargs = {
        'username': parser.get('Credentials', 'username'),
        'password': parser.get('Credentials', 'password'),
        'database': parser.get('Credentials', 'database_name'),
        'host': parser.get('Credentials', 'database_host'),
        'port': parser.get('Credentials', 'database_port'),
    }
    return URL(DB_TYPE, **kwargs)


# Attempt to create DB from default configuration file
__DEFAULT_PATH__ = os.path.join(
        '~', '.config', 'lightcurvedb', 'db.conf'
    )


def __register_process_guards__(engine):
    """Add SQLAlchemy process guards to the given engine"""
    @listens_for(engine, 'connect')
    def connect(dbapi_connection, connection_record):
        connection_record.info['pid'] = os.getpid()

    @listens_for(engine, 'checkout')
    def checkout(dbabi_connection, connection_record, connection_proxy):
        pid = os.getpid()
        if connection_record.info['pid'] != pid:
            # warnings.warn(
            #     'Parent process {} forked {} with an open database connection, '
            #     'which is being discarded and remade'.format(
            #         connection_record.info['pid'], pid
            #     )
            # )
            connection_record.connection = connection_proxy.connection = None
            raise DisconnectionError('Attempting to disassociate database connection')
    return engine


def __init_engine__(uri, **engine_kwargs):
    engine = create_engine(uri, **engine_kwargs)
    return __register_process_guards__(engine)


try:
    url = __config_to_url__(
        os.path.expanduser(__DEFAULT_PATH__)
    )
    __LCDB_ENGINE__ = __init_engine__(url, **DEFAULT_ENGINE_KWARGS)
    __SESSION_FACTORY__ = sessionmaker(bind=__LCDB_ENGINE__)

except KeyError:
    # Unknown config location, do not prepare default engine
    __LCDB_ENGINE__ = None
    __SESSION_FACTORY__ = None


def init_LCDB(uri, **kwargs):
    if 'pool_size' not in kwargs:
        kwargs['pool_size'] = 32
    if 'max_overflow' not in kwargs:
        kwargs['max_overflow'] = -1
    ENGINE = create_engine(uri, **kwargs)
    # Register engine with to allow for easy pooling dissociation
    ENGINE = __register_process_guards__(ENGINE)

    FACTORY = sessionmaker(bind=ENGINE)

    return FACTORY
