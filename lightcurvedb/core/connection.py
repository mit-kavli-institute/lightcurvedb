from __future__ import division, print_function

import contextlib
import os
import warnings
from time import sleep

from loguru import logger
from sqlalchemy import exc
from sqlalchemy.orm import sessionmaker

from lightcurvedb import models
from lightcurvedb.core import mixins
from lightcurvedb.core.engines import engine_from_config
from lightcurvedb.util.constants import __DEFAULT_PATH__


class ORM_DB(contextlib.AbstractContextManager):
    """
    Base Wrapper for all SQLAlchemy Session objects
    """

    def __init__(self, SessionMaker):
        self._sessionmaker = SessionMaker
        self._session_stack = []
        self._config = None
        self._max_depth = 10

    def __repr__(self):
        if self.is_active:
            return f"<DB status=open depth={self.depth}>"
        else:
            return "<DB status=closed>"

    def __enter__(self):
        """Enter into the context of an open SQLAlchemy session"""
        return self.open()

    def __exit__(self, exc_type, exc_value, traceback):
        """Exit from the current SQLAlchemy session"""
        self.close()

        return exc_type is None

    def open(self):  # noqa: B006
        """
        Establish a connection to the URI given to this instance. If the
        session has already been opened this function will cowardly exit.

        Returns
        -------
        ORM_DB
            Returns itself in an open state.
        """
        tries = 10
        wait = 1
        while tries > 1:
            try:
                if self.depth == 0:
                    session = self._sessionmaker()
                    self._session_stack.append(session)
                elif 0 < self.depth < self._max_depth:
                    nested_session = self.session.begin_nested()
                    self._session_stack.append(nested_session)
                    pass
                else:
                    raise RuntimeError(
                        "Database nested too far! Cowardly refusing"
                    )
                return self
            except exc.OperationalError as e:
                logger.warning(f"Could not connect: {e}, waiting for {wait}s")
                sleep(wait)
                wait *= 2
                tries -= 1

        raise RuntimeError("Could successfully connect to database")

    def close(self):
        """
        Closes the database connection. If this session has not been opened
        it will issue a warning.

        Returns
        -------
        ORM_DB
            Returns itself in a closed state.
        """
        try:
            if self.depth > 1:
                self.session.rollback()
                self._session_stack.pop()
            else:
                self._session_stack[0].close()
                self._session_stack.pop()
        except IndexError:
            warnings.warn(
                "DB session is not active. Ignoring duplicate close call"
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
        try:
            return self._session_stack[0]
        except IndexError:
            raise RuntimeError(
                "Session is not open. Please call `db_inst.open()`"
                "or use `with db_inst as opendb:`"
            )

    @property
    def bind(self):
        """
        Return the underlying SQLAlchemy Engine powering this connection.

        Returns
        -------
        sqlalchemy.Engine
            The Engine object powering the python side rendering of
            transactions.

        Raises
        ------
        RuntimeError
            Attempted to access this propery without first calling ``open()``.
        """
        if not self.is_active:
            raise RuntimeError(
                "Session is not open. Please call `db_inst.open()`"
                "or use `with db_inst as opendb:`"
            )
        return self.session.bind

    @property
    def config(self):
        """Return the config file path that is configuring this instance."""
        return self._config

    @property
    def depth(self):
        """
        What is the current transaction depth. For initially opened
        connections this will be 1. You can continue calling nested
        transactions until the max_depth amount is reached.

        Nested transaction scope and rules will follow PostgreSQL's
        implementation of SAVEPOINTS.

        Returns
        -------
        int
            The depth of transaction levels. 0 for closed transactions,
            1 for initially opened connections, and n < max_depth for
            nested transactions.
        """
        return len(self._session_stack)

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

        """
        return self.session.query(*args)

    def commit(self):
        """
        Commit the executed queries in the database to make any
        changes permanent.
        """
        self.session.commit()

    def rollback(self):
        """
        Rollback all changes to the previous commit.
        """
        self.session.rollback()

    def add(self, model_inst):
        """
        Adds the given QLPModel instance to be inserted into the database.

        Parameters
        ----------
        model_inst : QLPModel
            The new model to insert into the database.

        Raises
        ------
        sqlalchemy.IntegrityError
            Raised if the given instance violates given SQL constraints.
        """
        self.session.add(model_inst)

    def update(self, *args, **kwargs):
        """
        A helper method to ``db.session.update()``.

        Returns
        -------
        sqlalchemy.Query
        """
        self.session.update(*args, **kwargs)

    def delete(self, model_inst):
        """
        A helper method to ``db.session.delete()``.

        Parameters
        ----------
        model_inst : QLPModel
            The model to delete from the database.
        """
        self.session.delete(model_inst)

    def execute(self, *args, **kwargs):
        """
        Alias for db session execution. See sqlalchemy.Session.execute for
        more information.
        """
        return self.session.execute(*args, **kwargs)

    def flush(self):
        """
        Flush any pending queries to the remote database.
        """
        return self.session.flush()

    @property
    def is_active(self):
        """
        Is the DB object maintaining an active connection to a remote
        postgreSQL server?

        Returns
        -------
        bool
        """
        return self.depth > 0


class DB(
    ORM_DB,
    mixins.BestOrbitLightcurveAPIMixin,
    mixins.FrameAPIMixin,
    mixins.OrbitAPIMixin,
    mixins.ArrayOrbitLightcurveAPIMixin,
    mixins.PGCatalogMixin,
    mixins.QLPMetricAPIMixin,
):
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

    @property
    def orbits(self):
        """
        A quick property that aliases ``db.query(Orbit)``.

        Returns
        -------
        sqlalchemy.Query
        """
        return self.session.query(models.Orbit)

    @property
    def apertures(self):
        """
        A quick property that aliases ``db.query(Aperture)``.

        Returns
        -------
        sqlalchemy.Query
        """
        return self.session.query(models.Aperture)

    @property
    def lightcurves(self):
        """
        A quick property that aliases ``db.query(Lightcurve)``.

        Returns
        -------
        sqlalchemy.Query
        """
        return self.session.query(models.ArrayOrbitLightcurve)

    @property
    def lightcurve_types(self):
        """
        A quick property that aliases ``db.query(LightcurveType)``.

        Returns
        -------
        sqlalchemy.Query
        """
        return self.session.query(models.ArrayOrbitLightcurveType)


def db_from_config(config_path=None, db_class=None, **engine_kwargs):
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
    """
    engine = engine_from_config(
        os.path.expanduser(config_path if config_path else __DEFAULT_PATH__),
        **engine_kwargs,
    )

    db_class = DB if db_class is None else db_class

    factory = sessionmaker(bind=engine)

    new_db = db_class(factory)
    new_db._config = config_path
    return new_db


# Try and instantiate "global" lcdb
try:
    db = db_from_config()
except KeyError:
    db = None
