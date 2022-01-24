"""
This module describes various decorators that allow for easy construction
of scoped functions to reduce the amount of boilerplate needed as well as
encouraging developers to better encapsulate processing vs IO.
"""

from contextlib import contextmanager
from functools import wraps

from loguru import logger
from sqlalchemy.exc import InternalError

from lightcurvedb import db_from_config


def db_scope(application_name=None, config_override=None, **connection_kwargs):
    """
    Wrap a function within a database context.

    Functions encapsulated with this decorated will be passed an open database
    session. It is expected that the user will not prematurely close the
    session (although it is permitted, a warning will be emitted if such
    an event occurs).

    The user is also expected to commit any changes they wish to remain
    permanent. Upon return of the function all uncommitted changes are
    rolled back.

    Parameters
    ----------
    application_name: str, optional
        The application name to use as part of the Postgres connection.
    config_override: str or pathlike, optional
        Specify a configuration path that is not the default provided.
    """

    def _internal(func):
        app_name = application_name if application_name else func.__name__
        connect_args = connection_kwargs.pop("connect_args", {})
        connect_args["application_name"] = app_name

        @wraps(func)
        def wrapper(*args, **kwargs):
            func_results = None
            configured_db = db_from_config(
                config_path=config_override,
                connect_args=connect_args,
                **connection_kwargs
            )
            with configured_db as db_object:
                logger.debug(
                    f"Entering db context for {func} with {args} and {kwargs}"
                )
                func_results = func(db_object, *args, **kwargs)
                logger.debug(f"Exited db context for {func}")
                db_object.rollback()
            return func_results

        return wrapper

    return _internal


@contextmanager
def scoped_block(db, resource, acquire_actions=[], release_actions=[]):
    try:
        for action in acquire_actions:
            logger.debug(action)
            db.execute(action)
        db.commit()
        yield resource
    except InternalError:
        db.rollback()
    finally:
        for action in release_actions:
            logger.debug(action)
            db.execute(action)
        db.commit()
