"""
This module describes various decorators that allow for easy construction
of scoped functions to reduce the amount of boilerplate needed as well as
encouraging developers to better encapsulate processing vs IO.
"""

from contextlib import contextmanager
from functools import wraps

from loguru import logger
from sqlalchemy.exc import InternalError

from lightcurvedb.core.connection import LCDB_Session


def db_scope(session_factory=None, application_name=None, **session_kwargs):
    """
    Wrap a function within a database context.

    Functions encapsulated with this decorator will be passed an open database
    session. It is expected that the user will not prematurely close the
    session (although it is permitted, a warning will be emitted if such
    an event occurs).

    The user is also expected to commit any changes they wish to remain
    permanent. Upon return of the function all uncommitted changes are
    rolled back.

    Parameters
    ----------
    session_factory : sqlalchemy.orm.sessionmaker, optional
        A SQLAlchemy sessionmaker to use for creating sessions.
        Defaults to LCDB_Session if not provided.
    application_name : str, optional
        The application name to use for logging purposes. Note that this
        does not affect the PostgreSQL application_name as that is set at
        the engine level when the sessionmaker is configured.
    **session_kwargs : keyword arguments
        Additional arguments passed to the session factory when creating
        a new session. Common examples include 'bind' to override the
        engine, or 'info' to attach metadata to the session.
    """

    def _internal(func):
        # Use provided session factory or default to LCDB_Session
        _session_factory = (
            session_factory if session_factory is not None else LCDB_Session
        )

        # Set application name for PostgreSQL connection tracking
        app_name = application_name if application_name else func.__name__

        # Prepare session kwargs
        session_creation_kwargs = session_kwargs.copy()

        @wraps(func)
        def wrapper(*args, **kwargs):
            func_results = None
            # Create a new session using the factory with provided kwargs
            db_object = _session_factory(**session_creation_kwargs)
            try:
                logger.trace(
                    f"Entering db context for {app_name} ({func}) "
                    f"with {args} and {kwargs}"
                )
                func_results = func(db_object, *args, **kwargs)
                logger.trace(f"Exited db context for {app_name} ({func})")
                db_object.rollback()
            finally:
                db_object.close()
            return func_results

        return wrapper

    return _internal


@contextmanager
def scoped_block(db, resource, acquire_actions=[], release_actions=[]):
    try:
        for action in acquire_actions:
            logger.trace(action)
            db.execute(action)
        db.commit()
        yield resource
    except InternalError:
        db.rollback()
    finally:
        for action in release_actions:
            logger.trace(action)
            db.execute(action)
        db.commit()
