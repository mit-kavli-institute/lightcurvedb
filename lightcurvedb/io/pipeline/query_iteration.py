from functools import wraps

from lightcurvedb import db_from_config
from lightcurvedb.util.constants import DEFAULT_CONFIG_PATH


def map_query_to_function(
    query_function,
    application_name=None,
    config_override=None,
    **function_constants
):
    """
    A decorator which will execute the given query function and pass the
    results to the wrapped function.
    """

    def _internal(func):

        connect_args = {
            "application_name": func.__name__
            if application_name is None
            else application_name
        }

        @wraps(func)
        def wrapper(constraints):
            db = db_from_config(
                DEFAULT_CONFIG_PATH
                if config_override is None
                else config_override,
                connect_args=connect_args,
            )
            with db:
                query = query_function(*constraints)
                return func(db.execute(query).fetchall(), **function_constants)

        return wrapper

    return _internal
