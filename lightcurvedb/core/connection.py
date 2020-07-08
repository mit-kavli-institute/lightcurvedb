from __future__ import division, print_function

import os
import warnings
from sys import version_info

from configparser import ConfigParser

from sqlalchemy import Column, create_engine, and_
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
from lightcurvedb.core.engines import init_LCDB, __DEFAULT_PATH__
from lightcurvedb.managers.mass_upserts import MassUpsert


def engine_overrides(**engine_kwargs):
    if 'pool_size' not in engine_kwargs:
        engine_kwargs['pool_size'] = 12
    if 'max_overflow' not in engine_kwargs:
        engine_kwargs['max_overflow'] = -1
    if 'pool_pre_ping' not in engine_kwargs:
        engine_kwargs['pool_pre_ping'] = True
    if 'poolclass' not in engine_kwargs:
        engine_kwargs['poolclass'] = QueuePool
    return engine_kwargs


class DB(object):
    """Wrapper for SQLAlchemy sessions."""

    def __init__(self, FACTORY, SESSIONCLASS=None):

        if SESSIONCLASS:
            # Createdb instance opened
            self.SessionClass = SESSIONCLASS
            self._session = SESSIONCLASS()
            self._active = True
        else:
            self.__FACTORY__ = FACTORY
            self.SessionClass = scoped_session(self.__FACTORY__)
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
            return DB(None, SESSIONCLASS=self.SessionClass)
        else:
            warnings.warn(
                'DB session is already scoped, ignoring duplicate open call',
                RuntimeWarning
            )
        return self

    def close(self):
        if self._session is not None:
            self._session.close()
            self._session = None
            self._active = False

    @property
    def session(self):
        if not self._active:
            raise RuntimeError(
                'Session is not open. Please call db_inst.open() or use with db_inst as opendb:')
        return self._session

    def query(self, *args, **kwargs):
        return self._session.query(*args, **kwargs)

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
            q = q.filter(models.Lightcurve.lightcurve_type == lightcurve_type)
        else:
            q = q.filter(models.Lightcurve.lightcurve_type_id == lightcurve_type)

        if isinstance(aperture, models.Aperture):
            q = q.filter(models.Lightcurve.aperture == aperture)
        else:
            q = q.filter(models.Lightcurve.aperture_id == aperture)

        q = q.filter(
                models.Lightcurve.tic_id == tic,
                models.Lightcurve.cadence_type == cadence_type,
            )
        if resolve:
            return q.one()
        return q


    def lightcurves_from_tics(self, tics, **kw_filters):
        #pk_type = models.Lightcurve.tic_id.type
        #mq = MassQuery(
        #    self.session,
        #    models.Lightcurve,
        #    models.Lightcurve.tic_id,
        #    Column(pk_type, name='tic_id', primary_key=True, index=True),
        #    **kw_filters
        #)
        #mq.mass_insert(tics)
        #return mq.execute()
        q = self.lightcurves.filter(models.Lightcurve.tic_id.in_(tics)).filter_by(**kw_filters)
        return q

    def lightcurves_by_observation(self, orbit, camera=None, ccd=None):
        """
            Retrieve lightcurves that have been observed in the given orbit.
            This method can also filter by camera and ccd,

            Arguments:
                orbit {int, Orbit} -- The orbit to filter on. Can pass an
                integer representing the orbit number or an Orbit instance
                itself.
                camera {int} -- Filter by camera
                ccd {int} -- Filter by ccd
        """
        q = self.lightcurves.join(
            models.Observation,
            models.Lightcurve.tic_id == models.Observation.tic_id
        )
        if isinstance(orbit, models.Orbit):
            q = q.filter(models.Observation.orbit == orbit)
        elif isinstance(orbit, int):
            q = q.join(
                models.Orbit, models.Observation.orbit_id == models.Orbit.id
            )
            q = q.filter(models.Orbit.orbit_number == orbit)
        else:
            raise ValueError(
                'Cannot compare {} of type {} against the Orbit table.'.format(
                    orbit, type(orbit)
                )
            )
        if camera:
            q = q.filter(models.Observation.camera == camera)
        if ccd:
            q = q.filter(models.Observation.ccd == ccd)

        return q

    def lightcurves_from_best_aperture(self, q=None):
        if q is None:
            q = self.lightcurves
        q = q.join(
            models.BestApertureMap,
            and_(
                models.Lightcurve.tic_id == models.BestApertureMap.tic_id,
                models.Lightcurve.aperture_id == models.BestApertureMap.aperture_id
            )
        )
        return q

    def set_best_aperture(self, tic_id, aperture):
        upsert = models.BestApertureMap.set_best_aperture(tic_id, aperture)
        self._session.execute(upsert)

    def unset_best_aperture(self, tic_id, aperture):
        if isinstance(aperture, models.Aperture):
            check = self._session.query(models.BestApertureMap).get(
                (tic_id, aperture.name)
            )
        else:
            check = self._session.query(models.BestApertureMap).get(
                (tic_id, aperture)
            )
        if check:
            check.delete()

    def commit(self):
        self._session.commit()

    def add(self, model_inst):
        self._session.add(model_inst)

    def update(self, *args, **kwargs):
        self._session.update(*args, **kwargs)

    def delete(self, model_inst, synchronize_session='evaluate'):
        self._session.delete(
            model_inst, synchronize_session=synchronize_session
        )


def db_from_config(config_path=__DEFAULT_PATH__, **engine_kwargs):

    parser = ConfigParser()
    parser.read(config_path)

    kwargs = {
        'username': parser.get('Credentials', 'username'),
        'password': parser.get('Credentials', 'password'),
        'database': parser.get('Credentials', 'database_name'),
        'host': parser.get('Credentials', 'database_host'),
        'port': parser.get('Credentials', 'database_port'),
    }

    url = URL('postgresql+psycopg2', **kwargs)
    factory = init_LCDB(url, **engine_kwargs)
    db = DB(factory)
    db._config = config_path
    return db
