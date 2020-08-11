from __future__ import division, print_function

import os
import warnings
import numpy as np
from pandas import read_sql as pd_read_sql
from sys import version_info

from configparser import ConfigParser

from sqlalchemy import Column, SmallInteger, create_engine, and_, func, bindparam
from sqlalchemy.dialects.postgresql import aggregate_order_by
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
from lightcurvedb.core.engines import init_LCDB, __DEFAULT_PATH__
from lightcurvedb.core.quality_flags import set_quality_flags
from lightcurvedb.en_masse.temp_table import declare_lightcurve_cadence_map


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
    """Wrapper for SQLAlchemy sessions. This is the primary way to interface
    with the lightcurve database.

    It is advised not to instantiate this class directly. The preferred
    methods are through

    ::

        from lightcurvedb import db
        with db as opendb:
            foo

        # or
        from lightcurvedb import db_from_config
        db = db_from_config('path_to_config')

    """

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
        """Enter into the context of a SQLAlchemy open session"""
        return self.open()

    def __exit__(self, exc_type, exc_value, traceback):
        """Exit from the current SQLAlchemy session"""
        self.close()

    def open(self):
        """
        Establish a connection to the database. If this session has already
        been opened it will issue a warning before a no-op.

        Returns
        -------
        DB
            Returns itself in an open state.
        """
        if not self._active:
            if not self._session:
                self._session = self.SessionClass()
                self._active = True
                return self
        else:
            warnings.warn(
                'DB session is already scoped. Ignoring duplicate open call',
                RuntimeWarning
            )
        return self

    def close(self):
        """
        Closes the database connection. If this session has not been opened
        it will issue a warning.

        Returns
        -------
        DB
            Returns itself in a closed state.
        """
        if self._session is not None:
            self._session.close()
            self._session = None
            self._active = False
        else:
            warnings.warn(
                'DB session is not active. Ignoring duplicate close call'
            )
        return self

    @property
    def session(self):
        """
        Return the underlying SQLAlchemy Session.

        Returns
        -------
        sqlalchemy.orm.Session
            The active Session object performing all the interactions to
            PostgreSQL.

        Raises
        ------
        RuntimeError
            Attempting to access this property without first calling
            ``open()``.
        """
        if not self._active:
            raise RuntimeError(
                'Session is not open. Please call `db_inst.open()`'
                'or use `with db_inst as opendb:`'
            )
        return self._session

    def query(self, *args):
        """
        Constructs a query attached to this session.

        ::

            # Will retrive a list of Lightcurve objects
            db.query(Lightcurve)

            # Or

            # Will retrieve a list of tuples in the form of
            # (tic_id, list of cadences)
            db.query(Lightcurve.tic_id, Lightcurve.cadences)

            # More complicated queries can be made. But keep in mind
            # that queries spanning relations will require JOINing them
            # in order to retrieve the needed information
            db.query(
                Lightcurve.tic_id,
                Aperture.name
            ).join(
                Lightcurve.aperture
            )

        Arguments
        ---------
        *args : variadic Mapper or variadic Columns
            The parameters to query for. These parameters can be full
            mapper objects such as Lightcurve or Aperture. Or they can
            also be columns of these mapper objects such as Lightcurve.tic_id,
            or Aperture.inner_radius.

        Returns
        -------
        sqlalchemy.orm.query.Query
            Returns the Query object.

        Notes
        -----
        See .. _SQLAlchemy Query Docs: https://docs.sqlalchemy.org/en/13/orm/query.html
        """
        return self._session.query(*args)

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
        ).order_by(
            models.Frame.cadence.asc()
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
        ).order_by(
            models.Frame.cadence.asc()       
        ).all()

        return np.array(
            values, dtype=FRAME_COMP_DTYPE
        )

    def query_all_orbit_ids(self):
        return self.query(models.Orbit.orbit_number).order_by(
            models.Orbit.orbit_number.asc()
        ).all()

    # Begin Lightcurve Methods
    def query_lightcurves(self, tics=[], apertures=[], types=[]):
        """
        Make a query of lightcurves that meet the provided
        parameters. The emitted SQL clause is an IN statement for the given
        tics, apertures, and types (grouped using an AND).

        Arguments
        ---------
        tics : list, optional
            Filter lightcurves that have TIC identifiers contained in this
            list. If this list is empty then no filter will be applied using
            ``tics``.
        apertures : list, optional
            Filter lightcurves that have one of the given ``Aperture.name``
            strings. If this list is empty then no filter will be applied
            using ``apertures``.
        types : list, optional
            Filter lightcurves that have of of the given
            ``LightcurveType.name`` strings. If this list is empty then no
            filter will be applied using ``types``.

        Returns
        -------
        sqlalchemy.orm.query.Query
            A query of lightcurves that match the given parameters.
        """
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
        """
        A quick method to return a list of lightcurves that meet the provided
        parameters. The emitted SQL clause is an IN statement for the given
        tics, apertures, and types (grouped using an AND).

        Arguments
        ---------
        tics : list, optional
            Filter lightcurves that have TIC identifiers contained in this
            list. If this list is empty then no filter will be applied using
            ``tics``.
        apertures : list, optional
            Filter lightcurves that have one of the given ``Aperture.name``
            strings. If this list is empty then no filter will be applied
            using ``apertures``.
        types : list, optional
            Filter lightcurves that have of of the given
            ``LightcurveType.name`` strings. If this list is empty then no
            filter will be applied using ``types``.

        Returns
        -------
        list
            A list of lightcurves that match the given parameters. This list
            will be empty if no lightcurves match.

        """
        q = self.query_lightcurves(tics=tics, apertures=apertures, types=types)
        return q.all()

    def yield_from_db(self, chunksize, tics=[], apertures=[], types=[]):
        """
        Akin to ``load_from_db`` but instead of a list we return an iterator
        over a `server-side` PSQL cursor. This may be beneficial when
        interacting with hundreds of thousands of lightcurves or you don't
        need to load everything into memory at once.

        Arguments
        ---------
        chunksize : int
            The number of lightcurves the `PSQL` cursor should return as
            it executes the query. *NOTE* this is **NOT** the number of
            lightcurves returned each time the iterator yields. The
            iterator will return 1 lightcurve at a time.
        tics : list, optional
            Filter lightcurves that have TIC identifiers contained in this
            list. If this list is empty then no filter will be applied using
            ``tics``.
        apertures : list, optional
            Filter lightcurves that have one of the given ``Aperture.name``
            strings. If this list is empty then no filter will be applied
            using ``apertures``.
        types : list, optional
            Filter lightcurves that have of of the given
            ``LightcurveType.name`` strings. If this list is empty then no
            filter will be applied using ``types``.

        Returns
        -------
        iter
            An iterator over the given query that returns 1 row (lightcurve)
            at a time.

        Notes
        -----
        Server-side cursors are not free and have higher memory requirements
        for fileservers given certain circumstances than `normal` queries.
        This method is best reserved for very large queries.
        """
        q = self.query_lightcurves(tics=tics, apertures=apertures, types=types)
        return q.yield_per(chunksize)

    def get_lightcurve(self, tic, lightcurve_type, aperture, resolve=True):
        """
        Retrieves a single lightcurve row.

        Arguments
        ---------
        tic : int
            The TIC identifier of the lightcurve.
        aperture : str
            The aperture name of the lightcurve.
        lightcurve_type : str
            The name of the lightcurve's type.
        resolve : bool, optional
            If True return a single Lightcurve object, or a query instance.

        Returns
        -------
        Lightcurve or sqlalchemy.orm.query.Query
            Returns either a single Lightcurve instance or a Query object.

        Raises
        ------
        sqlalchemy.orm.exc.NoResultFound
            No lightcurve matched your requirements.

        """
        q = self.lightcurves.filter(
            models.Lightcurve.tic_id == tic,
            models.Lightcurve.aperture_id == aperture,
            models.Lightcurve.lightcurve_type_id == lightcurve_type
        )

        if resolve:
            return q.one()
        return q

    def lightcurves_from_tics(self, tics, **kw_filters):
        """
        Retrieves lightcurves from a collection of TIC identifiers.
        Can also apply keyword filters.

        .. deprecated:: 0.9.0
            ``kw_filters`` will be replaced instead with ``resolve`` to
            allow filters that span relationships.

        Arguments
        ---------
        tics : list or collection of integers
            The set of tics to filter for.
        **kw_filters : Keyword arguments, optional
            Keyword arguments to pass into ``filter_by``.
        """
        q = self.lightcurves.filter(models.Lightcurve.tic_id.in_(tics)).filter_by(**kw_filters)
        return q

    def tics_by_orbit(self, orbit_numbers, cameras=None, ccds=None, resolve=True, unique=True, sort=True):
        """
        Return tics by observed in the given orbit numbers. This query can be
        filtered for specific cameras/ccds.

        Arguments
        ---------
        orbit_numbers : int or iterable of integers
            The orbit numbers to discriminate for.
        cameras : list of integers, optional
            A list of cameras to discriminate for. If None (default) then no
            cameras will be filtered for.
        ccds : list of integers, optional
            A list of ccds to discriminate for. If None (default) then no
            ccds will be filtered for.
        resolve : bool, optional
            If True, resolve the query into a list of integers. If False
            return the ``sqlalchemy.orm.Query`` object representing
            the intended SQL statement.
        unique : bool, optional
            Since the same TIC can appear across multiple observations
            and JOIN statements produce cross products of Tables, query
            results might contain duplicate TICs. Setting ``unique`` to
            ``True`` will make the return be a proper set of TICs.
        sort : bool, optional
            If ``True`` apply an Ascending sort to the return.

        Returns
        -------
        list of integers or ``sqlalchemy.orm.Query``
            Returns either the result of the query or the Query object itself.
        """

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

        if sort:
            q = q.order_by(models.Observation.tic_id.asc())

        if resolve:
            return [r for r, in q.all()]
        return q

    def tics_by_sector(self, sectors, cameras=None, ccds=None, resolve=True, unique=True, sort=True):
        """
        Return tics by observed in the given sector numbers. This query can be
        filtered for specific cameras/ccds.

        Arguments
        ---------
        sectors : int or iterable of integers
            The sectors to discriminate for.
        cameras : list of integers, optional
            A list of cameras to discriminate for. If None (default) then no
            cameras will be filtered for.
        ccds : list of integers, optional
            A list of ccds to discriminate for. If None (default) then no
            ccds will be filtered for.
        resolve : bool, optional
            If True, resolve the query into a list of integers. If False
            return the ``sqlalchemy.orm.Query`` object representing
            the intended SQL statement.
        unique : bool, optional
            Since the same TIC can appear across multiple observations
            and JOIN statements produce cross products of Tables, query
            results might contain duplicate TICs. Setting ``unique`` to
            ``True`` will make the return be a proper set of TICs.
        sort : bool, optional
            If ``True`` apply an Ascending sort to the return.

        Returns
        -------
        list of integers or ``sqlalchemy.orm.Query``
            Returns either the result of the query or the Query object itself.
        """

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
        if sort:
            q = q.order_by(models.Observation.tic_id.asc())

        if resolve:
            return [r for r, in q.all()]
        return q

    def lightcurves_by_orbit(self, orbit_numbers, cameras=None, ccds=None, resolve=True):
        """
        Retrieve lightcurves that have been observed in the given
        orbit numbers. This method can also filter by camera and ccd.

        Arguments
        ---------
        orbit_numbers : list of integers
            The orbits to filter on.
        cameras : list of integers, optional
            List of cameras to query against. If None, then don't
            discriminate using cameras.
        ccds : list of integers, optional
            List of ccds to query against. If None, then don't discriminate
            using ccds
        resolve : bool, optional
            If True, resolve the query into a list of Lightcurves. If False
            return the ``sqlalchemy.orm.Query`` object representing
            the intended SQL statement.

        Returns
        -------
        list of lightcurves or ``sqlalchemy.orm.Query``
            Returns either the result of the query or the Query object itself.
        """

        tic_sub_q = self.tics_by_orbit(
            orbit_numbers,
            cameras=cameras,
            ccds=ccds,
            resolve=False,
            sort=False
        ).subquery('tics_from_observations')

        q = self.lightcurves.filter(models.Lightcurve.tic_id.in_(tic_sub_q))
        if resolve:
            return q.all()
        return q

    def lightcurves_by_sector(self, sectors, cameras=None, ccds=None, resolve=True):
        """
        Retrieve lightcurves that have been observed in the given
        sector numbers. This method can also filter by camera and ccd.

        Arguments
        ---------
        sectors : list of integers
            The sectors to filter on.
        cameras : list of integers, optional
            List of cameras to query against. If None, then don't
            discriminate using cameras.
        ccds : list of integers, optional
            List of ccds to query against. If None, then don't discriminate
            using ccds
        resolve : bool, optional
            If True, resolve the query into a list of Lightcurves. If False
            return the ``sqlalchemy.orm.Query`` object representing
            the intended SQL statement.

        Returns
        -------
        list of lightcurves or ``sqlalchemy.orm.Query``
            Returns either the result of the query or the Query object itself.

        """

        tic_sub_q = self.tics_by_sector(
            sectors, cameras=cameras, ccds=ccds, resolve=False, sort=False
        ).subquery('tics_from_observations')

        q = self.lightcurves.filter(models.Lightcurve.tic_id.in_(tic_sub_q))

        if resolve:
            return q.all()
        return q

    def lightcurves_from_best_aperture(self, q=None, resolve=True):
        """
        Find Lightcurve rows based upon their best aperture.

        Arguments
        ---------
        q : sqlalchemy.orm.query.Query, optional
            An initial Lightcurve Query. If left to None then all lightcurves
            will be queried for.
        resolve : bool, optional
            If True execute the Query into a list of Lightcurves. If False,
            return a Query object.

        Returns
        -------
        list of ``Lightcurves`` or a ``sqlalchemy.orm.query.Query``


        Notes
        -----
        This methods finds Lightcurves by JOINing them onto the BestAperture
        table. SQL JOINs find the cartesian product of two tables. This
        product is filtered by ``Lightcurve.tic_id == BestApertureMap.tic_id``
        and ``Lightcurve.aperture_id == BestApertureMap.aperture_id``. In this
        way the catesian product is filtered and the expected Best Aperture
        filter is achieved.

        For best results additional filters should be applied as this JOIN
        will, by default, attempt to find the cartesian product of the
        entire lightcurve and best aperture tables.

        This can be done by first passing in a ``sqlalchemy.orm.query.Query``
        object as the ``q`` parameter. This query must be on the
        ``Lightcurve`` table.

        ::

            # Example
            q = db.lightcurves_by_orbit(23, resolve=False) # Get init. query

            lcs = db.lightcurves_from_best_apertures(q=q)

            # Retrives lcs that appear in orbit 23 and filtered
            # for best aperture.
        """
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
        """
        Maps the best aperture to a TIC id.

        Arguments
        ---------
        tic_id : int
            The TIC id to assign an Aperture
        aperture : str or Aperture
            The name of an Aperture or an Aperture instance

        Notes
        -----
        These changes are not committed. The user will need to call
        ``db.commit()`` in order for the changes to be made
        permanent.
        """
        upsert = models.BestApertureMap.set_best_aperture(tic_id, aperture)
        self._session.execute(upsert)

    def unset_best_aperture(self, tic_id):
        """
        Unsets the best aperture.

        Arguments
        ---------
        tic_id : int
            The TIC id to unassign

        Notes
        -----
        If there is no best aperture map for the given TIC id then no
        operation will take place. Any changes will require ``db.commit()``
        to be made permanent.
        """
        check = self._session.query(models.BestApertureMap).filter(
            models.BestApertureMap.tic_id == tic_id
        ).one_or_none()
        if check:
            check.delete()

    def set_quality_flags(self, orbit_number, camera, ccd, cadences, quality_flags):
        """
        Assign quality flags en masse by orbit and camera and ccds. Updates
        are performed using the passed cadences and quality flag
        arrays.

        Arguments
        ---------
        orbit_number : int
            The orbit context for quality flag assignment.
        camera : int
            The camera context
        ccd : int
            The ccd context
        cadences : iterable of integers
            The cadences to key by to assign quality flags.
        quality_flags : iterable of integers
            The quality flags to assign in relation to the passed
            ``cadences``.

        Notes
        -----
        This method utilizes Temporary Tables which SQLAlchemy requires
        a clean session. Any present and uncommitted changes will be
        rolledback and a commit is emitted in order to construct the
        temporary tables.

        This automatically permanently changes the lightcurve models as it
        contains ``commit`` calls.

        """
        # Make a query of the relevant lightcurves
        q = self.query(
            models.Lightcurve.id,
        ).filter(
            models.Lightcurve.tic_id.in_(
                self.tics_by_orbit(
                    orbit_number,
                    cameras=[camera],
                    ccds=[ccd],
                    resolve=False
                ).subquery('tics')
            )
        )
        set_quality_flags(
            self.session,
            q,
            cadences,
            quality_flags
        )


    def commit(self):
        """
        Commit the executed queries in the database to make any
        changes permanent.
        """
        self._session.commit()

    def rollback(self):
        """
        Rollback all changes to the previous commit.
        """
        self._session.rollback()

    def add(self, model_inst):
        self._session.add(model_inst)

    def update(self, *args, **kwargs):
        self._session.update(*args, **kwargs)

    def delete(self, model_inst, synchronize_session='evaluate'):
        self._session.delete(
            model_inst, synchronize_session=synchronize_session
        )

    # Begin helper methods to quickly grab reference maps
    @property
    def observation_df(self):
        """
        Return an observation dataframe for client-side lookups. Best used to
        avoid large amounts of rapid hits to the server-side database.

        Returns
        -------
        pd.DataFrame
            A dataframe of ``tic_id``, the ``camera``, ``ccd``, and
            ``orbit.orbit_number`` that the tic was observed in.
        """
        q = self.query(
            models.Observation.tic_id, models.Observation.camera,
            models.Observation.ccd, models.Orbit.orbit_number
        ).join(
            models.Observation.orbit
        ).order_by(
            models.Orbit.orbit_number.asc(), models.Observation.tic_id.asc()
        )
        return pd_read_sql(q.statement, self.session.bind)


def db_from_config(config_path=__DEFAULT_PATH__, **engine_kwargs):
    """
    Create a DB instance from a configuration file.

    Arguments
    ---------
    config_path : str or Path, optional
        The path to the configuration file.
        Defaults to ``~/.config/lightcurvedb/db.conf``. This is expanded
        from the user's ``~`` space using ``os.path.expanduser``.
    **engine_kwargs : keyword arguments, optional
        Arguments to pass off into engine construction.
        See _SQLAlchemy Engine : https://docs.sqlalchemy.org/en/13/core/engines.html
    """
    parser = ConfigParser()
    parser.read(
        os.path.expanduser(
            config_path
        )
    )

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
