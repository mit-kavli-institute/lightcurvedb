from __future__ import division, print_function

import warnings
import os
from sqlalchemy import create_engine
from sqlalchemy.event import listens_for
from sqlalchemy.orm import scoped_session, sessionmaker, create_session
from sqlalchemy.exc import DisconnectionError


# Forward declare global engine
LCDB_ENGINE = None

def _register_process_guards(engine):
    @listens_for(engine, 'connect')
    def connect(dbapi_connection, connection_record):
        connection_record.info['pid'] = os.getpid()

    @listens_for(engine, 'checkout')
    def checkout(dbabi_connection, connection_record, connection_proxy):
        pid = os.getpid()
        if connection_record.info['pid'] != pid:
            warnings.warn(
                'Parent process {} forked {} with an open database connection, '
                'which is being discarded and remade'.format(
                    connection_record.info['pid'], pid
                )
            )
            connection_record.connection = connection_proxy.connection = None
            raise DisconnectionError('Attempting to disassociate database connection')
    return engine


def init_engine(uri, **kwargs):
    global LCDB_ENGINE

    # Prevent re-initialization of engines
    if LCDB_ENGINE is None:
        LCDB_ENGINE = create_engine(uri, **kwargs)
        # Register engine with to allow for easy pooling dissociation
        _register_process_guards(LCDB_ENGINE)

    return LCDB_ENGINE