from __future__ import division, print_function

import os
import warnings
from sys import version_info

from configparser import ConfigParser

from sqlalchemy import Column, create_engine
from sqlalchemy.event import listens_for
from sqlalchemy.exc import DisconnectionError
from sqlalchemy.orm import scoped_session, sessionmaker, joinedload

from lightcurvedb.core.base_model import QLPModel
from lightcurvedb import models
from lightcurvedb.util.uri import construct_uri, uri_from_config
from lightcurvedb.comparators.types import qlp_type_check, qlp_type_multiple_check
from lightcurvedb.en_masse import MassQuery

CONFIG_PATH = '~/.config/lightcurvedb/db.conf'

ENGINE = create_engine(
    uri_from_config(os.path.expanduser(CONFIG_PATH)),
    pool_size=48,
    pool_pre_ping=True,
    executemany_mode='values',
    executemany_values_page_size=10000,
    executemany_batch_page_size=500
)
FACTORY = sessionmaker(bind=ENGINE)
Session = scoped_session(FACTORY)

@listens_for(ENGINE, 'connect')
def connect(dbapi_connection, connection_record):
    connection_record.info['pid'] = os.getpid()

@listens_for(ENGINE, 'checkout')
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


class DB(object):
    """Wrapper for SQLAlchemy sessions."""

    def __init__(self, username, password, db_name, db_host, db_type='postgresql', port=5432, **engine_kwargs):
        self._uri = construct_uri(
            username, password, db_name, db_host, db_type, port
        )
        self._engine = ENGINE

        self.SessionClass = Session

        # Create any models which are not in the current PSQL schema
        self._session = None
        self._active = False
        self._config = None

    def __enter__(self):
        """Enter into the contejmxt of a SQLAlchemy open session"""
        self._engine.dispose()
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

    @property
    def orbits(self):
        return self.session.query(models.Orbit)

    @property
    def apertures(self):
        return self.session.query(models.Aperture)

    @property
    def lightcurves(self):
        return self.session.query(models.Lightcurve)

    @property
    def lightcurve_types(self):
        return self.session.query(models.LightcurveType)

    def query_lightcurves(self, tics=None, apertures=None, types=None, cadence_types=[30]):
        # Ensure lightcurves are JOINED with their lightpoints
        q = self.lightcurves.options(joinedload('lightpoints'))
        if apertures is not None:
            q = q.filter(qlp_type_multiple_check(models.Aperture, apertures))
        if types is not None:
            q = q.filter(qlp_type_multiple_check(models.LightcurveType, types))
        if cadence_types is not None:
            q = q.filter(models.Lightcurve.cadence_type.in_(cadence_types))
        if tics is not None:
            q = q.filter(models.Lightcurve.tic_id.in_(tics))
        return q

    def load_from_db(self, tics=None, apertures=None, types=None, cadence_types=[30]):
        q = self.query_lightcurves(tics=tics, apertures=apertures, types=types, cadence_types=cadence_types)
        return q.all()

    def yield_from_db(self, chunksize, tics=None, apertures=None, types=None, cadence_types=[30]):
        q = self.query_lightcurves(tics=tics, apertures=apertures, types=types, cadence_types=cadence_types)
        return q.yield_per(chunksize)

    def get_lightcurve(self, tic, lightcurve_type, aperture, cadence_type=30, resolve=True):
        q = self.lightcurves

        if isinstance(lightcurve_type, models.LightcurveType):
            q = q.filter(models.Lightcurve.lightcurve_type_id == lightcurve_type.id)
        else:
            x = self.session.query(models.LightcurveType).filter(models.LightcurveType.name == lightcurve_type).one()
            q = q.filter(models.Lightcurve.lightcurve_type_id == x.id)

        if isinstance(aperture, models.Aperture):
            q = q.filter(models.Lightcurve.aperture_id == aperture.id)
        else:
            x = self.session.query(models.Aperture).filter(models.Aperture.name == aperture).one()
            q = q.filter(models.Lightcurve.aperture_id == x.id)

        q = q.filter(
                models.Lightcurve.tic_id == tic,
                models.Lightcurve.cadence_type == cadence_type,
            )
        if resolve:
            return q.one()
        return q


    def lightcurves_from_tics(self, tics, w_lightpoints=False, **kw_filters):
        pk_type = models.Lightcurve.tic_id.type
        mq = MassQuery(
            self.session,
            models.Lightcurve,
            models.Lightcurve.tic_id,
            Column(pk_type, name='tic_id', primary_key=True, index=True),
            **kw_filters
        )
        for tic in tics:
            mq.insert(tic_id=tic)
        if w_lightpoints:
            return mq.execute().options(joinedload('lightpoints'))
        return mq.execute()

    def commit(self):
        self._session.commit()

    def add(self, model_inst):
        self._session.add(model_inst)

    def update(self, *args, **kwargs):
        self._session.update(*args, **kwargs)

    def delete(self, model_inst, synchronize_session='evaluate'):
        self._session.delete(model_inst, synchronize_session=synchronize_session)


def db_from_config(config_path, **engine_kwargs):
    parser = ConfigParser()
    parser.read(config_path)

    kwargs = {
        'username': parser.get('Credentials', 'username'),
        'password': parser.get('Credentials', 'password'),
        'db_name': parser.get('Credentials', 'database_name'),
        'db_host': parser.get('Credentials', 'database_host'),
        'port': parser.get('Credentials', 'database_port'),
    }

    db = DB(
        parser.get('Credentials', 'username'),
        parser.get('Credentials', 'password'),
        parser.get('Credentials', 'database_name'),
        parser.get('Credentials', 'database_host'),
        parser.get('Credentials', 'database_port'),
        **engine_kwargs)
    db._config = config_path
    return db
