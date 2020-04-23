from __future__ import division, print_function

import os
import warnings
from sys import version_info

from configparser import ConfigParser

from sqlalchemy import Column, create_engine
from sqlalchemy.pool import QueuePool
from sqlalchemy.event import listens_for
from sqlalchemy.exc import DisconnectionError
from sqlalchemy.orm import scoped_session, sessionmaker, joinedload
from sqlalchemy.engine.url import URL

from lightcurvedb.core.base_model import QLPModel
from lightcurvedb import models
from lightcurvedb.util.uri import construct_uri, uri_from_config
from lightcurvedb.comparators.types import qlp_type_check, qlp_type_multiple_check
from lightcurvedb.en_masse import MassQuery
from lightcurvedb.core.engines import init_engine



class DB(object):
    """Wrapper for SQLAlchemy sessions."""

    def __init__(self, username, password, db_name, db_host, db_type='postgresql+psycopg2', port=5432, **engine_kwargs):
        if password and len(password) == 0:
            password = None
        self._url = URL(db_type, username=username, password=password, host=db_host, port=port, database=db_name)
        self._engine = init_engine(
            self._url,
            pool_size=48,
            max_overflow=16,
            poolclass=QueuePool,
            pool_pre_ping=True,
            **engine_kwargs
        )
        self.factory = sessionmaker(bind=self._engine)
        self.SessionClass = scoped_session(self.factory)

        # Create any models which are not in the current PSQL schema
        self._session = None
        self._active = False
        self._config = None

    def __enter__(self):
        """Enter into the contejmxt of a SQLAlchemy open session"""
        return self.open()

    def __exit__(self, exc_type, exc_value, traceback):
        """Exit from the current SQLAlchemy session"""
        self.close()

    def open(self):
        if not self._active:
            self._session = self.SessionClass()
            self._active = True
        else:
            warnings.warn(
                'DB session is already scoped, ignoring duplicate open call',
                RuntimeWarning
            )
        return self

    def close(self):
        if self._active:
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
        if not self._active:
            raise RuntimeError(
                'Session is not open. Please call db_inst.open() or use with db_inst as opendb:')
        return self._session

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

    def query_lightcurves(self, tics=[], apertures=[], types=[], cadence_types=[30]):
        q = self.lightcurves
        if len(apertures) > 0:
            q = q.filter(
                models.Lightcurve.aperture_id.in_(
                    qlp_type_multiple_check(self, models.Aperture, apertures)
                )
            )
        if len(types) > 0:
            q = q.filter(
                models.Lightcurve.lightcurve_type_id.in_(
                    qlp_type_multiple_check(self, models.LightcurveType, types)
                )
            )
        if len(cadence_types) > 0:
            q = q.filter(models.Lightcurve.cadence_type.in_(cadence_types))

        if len(tics) > 0:
            q = q.filter(models.Lightcurve.tic_id.in_(tics))
        return q

    def load_from_db(self, tics=[], apertures=[], types=[], cadence_types=[30]):
        q = self.query_lightcurves(tics=tics, apertures=apertures, types=types, cadence_types=cadence_types)
        return q.all()

    def yield_from_db(self, chunksize, tics=[], apertures=[], types=[], cadence_types=[30]):
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


    def lightcurves_from_tics(self, tics, **kw_filters):
        pk_type = models.Lightcurve.tic_id.type
        mq = MassQuery(
            self.session,
            models.Lightcurve,
            models.Lightcurve.tic_id,
            Column(pk_type, name='tic_id', primary_key=True, index=True),
            **kw_filters
        )
        mq.mass_insert(tics)
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
    if 'port' not in engine_kwargs:
        engine_kwargs['port'] = kwargs['port']

    db = DB(
        kwargs['username'],
        kwargs['password'],
        kwargs['db_name'],
        kwargs['db_host'],
        **engine_kwargs)
    db._config = config_path
    return db
