import configurables as conf
from sqlalchemy import NullPool, pool
from sqlalchemy.orm import sessionmaker

from lightcurvedb.core.engines import thread_safe_engine
from lightcurvedb.util.constants import DEFAULT_CONFIG_PATH


@conf.configurable("Credentials")
@conf.param("database_name")
@conf.param("username")
@conf.param("password")
@conf.option("database_host", default="localhost")
@conf.option("database_port", type=int, default=5432)
@conf.option("dialect", default="postgresql+psycopg")
def db_from_config(
    database_name,
    username,
    password,
    database_host,
    database_port,
    dialect,
    **engine_kwargs
):
    """
    Create a DB instance from a configuration file.

    Arguments
    ---------
    config_path : str or Path, optional
        The path to the configuration file.
        Defaults to ``~/.config/lightcurvedb/db.conf``. This is expanded
        from the user's ``~`` space using ``pathlib.Path().expanduser()``.
    **engine_kwargs : keyword arguments, optional
        Arguments to pass off into engine construction.
    """
    engine = thread_safe_engine(
        database_name,
        username,
        password,
        database_host,
        database_port,
        dialect,
        poolclass=pool.NullPool,
        **engine_kwargs
    )
    session = sessionmaker(bind=engine)()
    return session


@conf.configurable("Credentials")
@conf.param("database_name")
@conf.param("username")
@conf.param("password")
@conf.option("database_host", default="localhost")
@conf.option("database_port", default=5432)
def configure_engine(
    username, password, database_name, database_host, database_port
):
    engine = thread_safe_engine(
        database_name,
        username,
        password,
        database_host,
        database_port,
        "postgresql+psycopg",
        poolclass=NullPool,
    )
    return engine


LCDB_Session = sessionmaker(expire_on_commit=False)

# Try and instantiate "global" lcdb
if not DEFAULT_CONFIG_PATH.exists():
    db = None
else:
    LCDB_Session.configure(bind=configure_engine(DEFAULT_CONFIG_PATH))
    db = LCDB_Session()
