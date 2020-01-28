from __future__ import division, print_function

import os
import warnings
from sys import version_info

from configparser import ConfigParser

from sqlalchemy import Column, create_engine
from sqlalchemy.event import listens_for
from sqlalchemy.exc import DisconnectionError
from sqlalchemy.orm import scoped_session, sessionmaker

from lightcurvedb.core.base_model import QLPModel
from lightcurvedb import models
from lightcurvedb.util.uri import construct_uri


def connect(dbapi_connection, connection_record):
    connection_record.info['pid'] = os.getpid()

def checkout(dbabi_connection, connection_record, connection_proxy):
    pid = os.getpid()
    if connection_record.info['pid'] != pid:
        warnings.warn(
            'Parent process {} forked {} with an open database connection, '
            'which is being discarded and remade'.format(
                connection_record['pid'], pid
            )
        )
        connection_record.connection = connection_proxy.connection = None
        raise DisconnectionError('Attempting to disassociate database connection')


class DB(object):
    """Wrapper for SQLAlchemy sessions."""

    def __init__(self, username, password, db_name, db_host, db_type='postgresql', port=5432):
        self._uri = construct_uri(
            username, password, db_name, db_host, db_type, port
        )

        self._engine = create_engine(self.uri, pool_size=20, max_overflow=48)
        listens_for(self._engine, 'connect', connect)
        listens_for(self._engine, 'checkout', checkout)

        self.session_factory = sessionmaker(bind=self._engine)
        self.SessionClass = scoped_session(self.session_factory)

        # Create any models which are not in the current PSQL schema
        QLPModel.metadata.create_all(self._engine)
        self._session = None
        self._active = False

    def __enter__(self):
        """Enter into the contejmxt of a SQLAlchemy open session"""
        return self.open()

    def __exit__(self, exc_type, exc_value, traceback):
        """Exit from the current SQLAlchemy session"""
        self.close()

    def open(self):
        if self._session is None:
            self._session = self.SessionClass()
            self._active = True
        else:
            warnings.warn(
                'DB session is already scoped, ignoring duplicate open call',
                RuntimeWarning
            )
        return self

    def close(self):
        if self._session is not None:
            self.SessionClass.remove()
            self._session = None
            self._active = False
        else:
            warnings.warn(
                'DB session is already closed',
                RuntimeWarning
            )
        return self

    @property
    def session(self):
        return self._session

    @property
    def uri(self):
        return self._uri

    @property
    def is_active(self):
        return self._active

    def commit(self):
        self._session.commit()

    def add(self, model_inst):
        self._session.add(model_inst)

    def update(self, *args, **kwargs):
        self._session.update(*args, **kwargs)

    def delete(self, model_inst, synchronize_session='evaluate'):
        self._session.delete(model_inst, synchronize_session=synchronize_session)


def db_from_config(config_path):
    parser = ConfigParser()
    parser.read(config_path)

    kwargs = {
        'username': parser.get('Credentials', 'username'),
        'password': parser.get('Credentials', 'password'),
        'db_name': parser.get('Credentials', 'database_name'),
        'db_host': parser.get('Credentials', 'database_host'),
        'port': parser.get('Credentials', 'database_port'),
    }

    return DB(**kwargs)
