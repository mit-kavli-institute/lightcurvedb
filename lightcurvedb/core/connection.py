import configurables as conf
from sqlalchemy import pool
from sqlalchemy.orm import Session

from lightcurvedb import models
from lightcurvedb.core import mixins
from lightcurvedb.core.engines import thread_safe_engine
from lightcurvedb.util.constants import __DEFAULT_PATH__


class DB(
    Session,
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
@conf.option("dialect", default="postgresql+psycopg2")
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
    return DB(engine)


# Try and instantiate "global" lcdb
try:
    db = db_from_config(__DEFAULT_PATH__)
except KeyError:
    db = None
