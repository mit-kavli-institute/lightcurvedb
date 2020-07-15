from __future__ import division, print_function

import os
import warnings
import numpy as np
from sys import version_info

from configparser import ConfigParser

from sqlalchemy import Column, create_engine, and_, func
from sqlalchemy.pool import QueuePool
from sqlalchemy.event import listens_for
from sqlalchemy.exc import DisconnectionError
from sqlalchemy.orm import scoped_session, sessionmaker, joinedload
from sqlalchemy.engine.url import URL

from lightcurvedb.core.base_model import QLPModel
from lightcurvedb import models
from lightcurvedb.models.orbit import ORBIT_DTYPE
from lightcurvedb.models.frame import FRAME_DTYPE
from lightcurvedb.util.uri import construct_uri, uri_from_config
from lightcurvedb.util.type_check import isiterable
from lightcurvedb.comparators.types import qlp_type_check, qlp_type_multiple_check
from lightcurvedb.en_masse import MassQuery
from lightcurvedb.core.engines import init_LCDB, __DEFAULT_PATH__
from lightcurvedb.managers.mass_upserts import MassUpsert


# Bring legacy capability
# TODO Encapsulate so it doesn't pollute this namespace
LEGACY_FRAME_TYPE_ID = 'Raw FFI'
FRAME_COMP_DTYPE = [('orbit_id', np.int32)] + FRAME_DTYPE


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
            if not self._session:
                self._session = self.SessionClass()
                self._active = True
                return self
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

    # Begin orbit methods
    def query_orbits_by_id(self, orbit_numbers):
        """Grab a numpy array representing the orbits"""
        orbits = self.query(*models.Orbit.get_legacy_attrs())\
                .filter(models.Orbit.orbit_number.in_(orbit_numbers))\
                .order_by(models.Orbit.orbit_number)
        return np.array(orbits.all(), dtype=ORBIT_DTYPE)

    def query_orbit_cadence_limit(self, orbit_id, cadence_type, camera, frame_type=LEGACY_FRAME_TYPE_ID):
        cadence_limit = self.query(
            func.min(models.Frame.cadence), func.max(models.Frame.cadence)
        ).join(models.Orbit, models.Frame.orbit_id == models.Orbit.id).filter(
            models.Frame.cadence_type == cadence_type,
            models.Frame.camera == camera,
            models.Frame.frame_type_id == frame_type,
            models.Orbit.orbit_number == orbit_id
        )

        return cadence_limit.one()

    def query_orbit_tjd_limit(self, orbit_id, cadence_type, camera):
        tjd_limit = self.query(
            func.min(models.Frame.start_tjd), func.max(models.Frame.end_tjd)
        ).join(models.Frame.orbit).filter(
            models.Frame.cadence_type == cadence_type,
            models.Frame.camera == camera,
            models.Orbit.orbit_number == orbit_id
        )

        return tjd_limit.one()

    def query_frames_by_orbit(self, orbit_id, cadence_type, camera):
        # Differs from PATools in that orbit_id != orbit number
        # so we need to record that.
        cols = [models.Orbit.orbit_number] + list(models.Frame.get_legacy_attrs())
        values = self.query(
            *cols
        ).join(models.Frame.orbit).filter(
            models.Frame.cadence_type == cadence_type,
            models.Frame.camera == camera,
            models.Orbit.orbit_number == orbit_id
        ).all()

        return np.array(
            values, dtype=FRAME_COMP_DTYPE
        )

    def query_frames_by_cadence(self, camera, cadence_type, cadences):
        cols = [models.Orbit.orbit_number] + list(models.Frame.get_legacy_attrs())
        values = self.query(
            *cols
        ).join(models.Frame.orbit).filter(
            models.Frame.cadence_type == cadence_type,
            models.Frame.camera == camera,
            models.Frame.cadence.in_(cadences)
        ).all()

        return np.array(
            values, dtype=FRAME_COMP_DTYPE
        )

    def query_all_orbit_ids(self):
        return self.query(models.Orbit.orbit_number).all()

    # Begin Lightcurve Methods
    def query_lightcurves(self, tics=[], apertures=[], types=[]):
        q = self.lightcurves
        if len(apertures) > 0:
            q = q.filter(
                models.Lightcurve.aperture_id.in_(
                    apertures
                )
            )
        if len(types) > 0:
            q = q.filter(
                models.Lightcurve.lightcurve_type_id.in_(
                    types
                )
            )
        if len(tics) > 0:
            q = q.filter(models.Lightcurve.tic_id.in_(tics))
        return q

    def load_from_db(self, tics=[], apertures=[], types=[]):
        q = self.query_lightcurves(tics=tics, apertures=apertures, types=types)
        return q.all()

    def yield_from_db(self, chunksize, tics=[], apertures=[], types=[]):
        q = self.query_lightcurves(tics=tics, apertures=apertures, types=types)
        return q.yield_per(chunksize)

    def get_lightcurve(self, tic, lightcurve_type, aperture, resolve=True):
        q = self.lightcurves.filter(
            models.Lightcurve.tic_id == tic,
            models.Lightcurve.aperture_id == aperture,
            models.Lightcurve.lightcurve_type_id == lightcurve_type
        )

        if resolve:
            return q.one()
        return q


    def lightcurves_from_tics(self, tics, **kw_filters):
        q = self.lightcurves.filter(models.Lightcurve.tic_id.in_(tics)).filter_by(**kw_filters)
        return q

    def tics_by_orbit(self, orbit_numbers, cameras=None, ccds=None, resolve=True, unique=True):

        if not isiterable(orbit_numbers):
            orbit_numbers = [orbit_numbers]

        col = models.Observation.tic_id.distinct() if unique else models.Observation.tic_id

        q = self.query(
            col
        ).join(
            models.Observation.orbit
        ).filter(
            models.Orbit.orbit_number.in_(orbit_numbers)
        )

        if cameras:
            q = q.filter(models.Observation.camera.in_(cameras))
        if ccds:
            q = q.filter(models.Observation.ccd.in_(ccds))

        if resolve:
            return [r for r, in q.all()]
        return q

    def tics_by_sector(self, sectors, cameras=None, ccds=None, resolve=True, unique=True):

        if not isiterable(sectors):
            sectors = [sectors]

        col = models.Observation.tic_id.distinct() if unique else models.Observation.tic_id

        q = self.query(
            col
        ).join(
            models.Observation.orbit
        ).filter(
            models.Orbit.sector.in_(sectors)
        )

        if cameras:
            q = q.filter(models.Observation.camera.in_(cameras))
        if ccds:
            q = q.filter(models.Observation.ccd.in_(ccds))

        if resolve:
            return [r for r, in q.all()]
        return q

    def lightcurves_by_orbit(self, orbits_numbers, cameras=None, ccds=None):
        """
            Retrieve lightcurves that have been observed in the given orbit.
            This method can also filter by camera and ccd.

            Arguments:
                orbit {int, Orbit} -- The orbit to filter on. Can pass an
                integer representing the orbit number or an Orbit instance
                itself.
                cameras (list) -- List of cameras to query against. If None,
                then don't discriminate using cameras.
                ccds (list) -- List of ccds to query against. If None,
                then don't discriminate using ccds
        """

        tic_sub_q = self.tics_by_orbit(
            orbit_numbers, cameras=cameras, ccds=ccds, resolve=False
        ).subquery('tics_from_observations')

        return self.lightcurves.filter(models.Lightcurve.tic_id.in_(tic_sub_q))

    def lightcurves_by_sector(self, sectors, cameras=None, ccds=None):
        """
        Retrieve lightcurves that have been observed in the given sectors.
        This method can also filter by camera and ccd.

        Arguments:
            *sectors (int) -- The sector to filter for.
            cameras (list) -- List of cameras to query against. If None,
            then don't discriminate using cameras.
            ccds (list) -- List of ccds to query against. If None,
            then don't discriminate using ccds.
        """
        tic_sub_q = self.tics_by_sector(
            sectors, cameras=cameras, ccds=ccds, resolve=False
        ).subquery('tics_from_observations')

        return self.lightcurves.filter(models.Lightcurve.tic_id.in_(tic_sub_q))

    def lightcurves_from_best_aperture(self, q=None, resolve=True):
        if q is None:
            q = self.lightcurves
        q = q.join(
            models.BestApertureMap,
            and_(
                models.Lightcurve.tic_id == models.BestApertureMap.tic_id,
                models.Lightcurve.aperture_id == models.BestApertureMap.aperture_id
            )
        )
        if resolve:
            return q.all()
        return q

    def lightcurve_id_map(self, filters, resolve=True):
        q = self.query(
            models.Lightcurve.id,
            models.Lightcurve.tic_id,
            models.Lightcurve.aperture_id,
            models.Lightcurve.lightcurve_type_id
        ).filter(*filters)

        if resolve:
            return q.all()
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

    def set_quality_flags(self, orbit_number, camera, ccd, cadences, quality_flags):
        raise NotImplementedError

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
