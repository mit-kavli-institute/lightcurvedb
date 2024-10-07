import os

from sqlalchemy import create_engine
from sqlalchemy.event import listens_for
from sqlalchemy.exc import DisconnectionError


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


def thread_safe_engine(
    database_name,
    username,
    password,
    database_host,
    database_port,
    dialect,
    **engine_overrides,
):
    """
    Create an SQLAlchemy engine from the configuration path.
    """
    url = (
        f"{dialect}://{username}:{password}"  # noqa
        f"@{database_host}:{database_port}/{database_name}"  # noqa
    )
    engine = create_engine(url, **engine_overrides)
    return __register_process_guards__(engine)
