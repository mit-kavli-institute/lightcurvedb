from __future__ import division, print_function

import os

try:
    from configparser import ConfigParser, NoSectionError
except ImportError:
    # Python 2?
    from ConfigParser import ConfigParser, NoSectionError


from lightcurvedb.util.constants import __DEFAULT_PATH__

from sqlalchemy import create_engine
from sqlalchemy.event import listens_for
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import DisconnectionError
from sqlalchemy import pool
from psycopg2 import connect


def __config_to_kwargs__(path):
    parser = ConfigParser()
    parser.read(path)
    kwargs = {
        "username": parser.get("Credentials", "username"),
        "password": parser.get("Credentials", "password"),
        "database": parser.get("Credentials", "database_name"),
        "host": parser.get("Credentials", "database_host"),
        "port": parser.get("Credentials", "database_port"),
    }
    return kwargs


def __register_process_guards__(engine):
    """Add SQLAlchemy process guards to the given engine"""

    @listens_for(engine, "connect")
    def connect(dbapi_connection, connection_record):
        connection_record.info["pid"] = os.getpid()

    @listens_for(engine, "checkout")
    def checkout(dbabi_connection, connection_record, connection_proxy):
        pid = os.getpid()
        if connection_record.info["pid"] != pid:
            connection_record.connection = connection_proxy.connection = None
            raise DisconnectionError(
                "Attempting to disassociate database connection"
            )

    return engine


def __init_engine__(uri, **engine_kwargs):
    engine = create_engine(uri, **engine_kwargs)
    return __register_process_guards__(engine)


def psycopg_connection(uri_override=None):
    """
    Create a raw psycopg2 connection to a postgreSQL database using the
    provided configuration path or using lightcurvedb's default config
    path.

    Parameters
    ----------
    uri_override: pathlike, optional
        The path to a configuration file to configure a psycopg2 connection.
        If not provided then lightcurvedb will use the default user
        configuration path
    Returns
    -------
    psycopg2.Connection
    """
    kwargs = __config_to_kwargs__(
        uri_override if uri_override else __DEFAULT_PATH__
    )

    return connect(
        dbname=kwargs["database"],
        user=kwargs["username"],
        password=kwargs["password"],
        host=kwargs["host"],
        port=kwargs["port"],
    )


ENGINE_CONF_REQ_ARGS = {
    "username": "username",
    "password": "password",
    "database": "database_name",
}

ENGINE_CONF_OPT_ARGS = {
    "dialect": "dialect",
    "host": "database_host",
    "port": "database_port",
}

OPT_DEFAULTS = {
    "dialect": "postgresql+psycopg2",
    "host": "localhost",
    "port": 5432,
}


def engine_from_config(
    config_path,
    config_group="Credentials",
    uri_template="{dialect}://{username}:{password}@{host}:{port}/{database}",
    **engine_overrides
):
    """
    Create an SQLAlchemy engine from the configuration path.
    """
    config = ConfigParser()
    config.read(os.path.expanduser(config_path))

    section = config[config_group]

    kwargs = {}
    # Absolutely required
    for kwarg, config_path in ENGINE_CONF_REQ_ARGS.items():
        kwargs[kwarg] = section[config_path]

    for kwarg, config_path in ENGINE_CONF_OPT_ARGS.items():
        kwargs[kwarg] = section.get(config_path, OPT_DEFAULTS[kwarg])

    if "poolclass" not in engine_overrides:
        engine_overrides["poolclass"] = getattr(
            pool, kwargs.pop("poolclass", "NullPool")
        )

    url = uri_template.format(**kwargs)
    engine = __init_engine__(url, **engine_overrides)
    return __register_process_guards__(engine)
