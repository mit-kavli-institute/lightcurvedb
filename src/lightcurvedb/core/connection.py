import configurables as conf
from sqlalchemy import URL, NullPool, create_engine, pool
from sqlalchemy.orm import Session, sessionmaker

from lightcurvedb import models
from lightcurvedb.core import mixins
from lightcurvedb.core.engines import thread_safe_engine
from lightcurvedb.util.constants import DEFAULT_CONFIG_PATH


class DB(
    Session,
    mixins.ApertureAPIMixin,
    mixins.LightcurveTypeAPIMixin,
    mixins.BestOrbitLightcurveAPIMixin,
    mixins.BLSAPIMixin,
    mixins.FrameAPIMixin,
    mixins.OrbitAPIMixin,
    mixins.ArrayOrbitLightcurveAPIMixin,
    mixins.PGCatalogMixin,
    mixins.QLPMetricAPIMixin,
    mixins.LegacyAPIMixin,
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

    def __init__(self, *args, **kwargs):
        config = kwargs.pop("config", None)
        super().__init__(*args, **kwargs)
        self.config = config

    @property
    def orbits(self):
        """
        A quick property that aliases ``db.query(Orbit)``.

        Returns
        -------
        sqlalchemy.Query
        """
        return self.query(models.Orbit)

    @property
    def apertures(self):
        """
        A quick property that aliases ``db.query(Aperture)``.

        Returns
        -------
        sqlalchemy.Query
        """
        return self.query(models.Aperture)

    @property
    def lightcurves(self):
        """
        A quick property that aliases ``db.query(Lightcurve)``.

        Returns
        -------
        sqlalchemy.Query
        """
        return self.query(models.Lightcurve)

    @property
    def lightcurve_types(self):
        """
        A quick property that aliases ``db.query(LightcurveType)``.

        Returns
        -------
        sqlalchemy.Query
        """
        return self.query(models.LightcurveType)


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
    session = Session(bind=engine)
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
    url = URL.create(
        "postgresql+psycopg",
        database=database_name,
        username=username,
        password=password,
        host=database_host,
        port=database_port,
    )
    engine = create_engine(url, poolclass=NullPool)
    return engine


LCDB_Session = sessionmaker(expire_on_commit=False, class_=DB)

# Try and instantiate "global" lcdb
if not DEFAULT_CONFIG_PATH.exists():
    db = None
else:
    LCDB_Session.configure(bind=configure_engine(DEFAULT_CONFIG_PATH))
    db = LCDB_Session()
