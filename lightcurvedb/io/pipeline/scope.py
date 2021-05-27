"""
This module describes various decorators that allow for easy construction
of scoped functions to reduce the amount of boilerplate needed as well as
encouraging developers to better encapsulate processing vs IO.
"""

from functools import wraps
from lightcurvedb import db_from_config
from lightcurvedb.util.logger import lcdb_logger


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
            configured_db = (
                db_from_config(
                    config_path=config_override,
                    connect_args=connect_args,
                    **connection_kwargs
                )
            )
            with configured_db as db_object:
                lcdb_logger.debug(
                    "Entering db context for {0} with {1} and {2}".format(
                        func,
                        args,
                        kwargs
                    )
                )
                func_results = func(db_object, *args, **kwargs)
                lcdb_logger.debug("Exited db context for {0}".format(func))
                db_object.rollback()
            return func_results

        return wrapper

    return _internal
