import os

from sqlalchemy import Session
from sqlalchemy.orm import sessionmaker

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
    engine = thread_safe_engine(
        os.path.expanduser(config_path if config_path else __DEFAULT_PATH__),
        **engine_kwargs,
    )

    db_class = DB if db_class is None else db_class

    factory = sessionmaker(bind=engine, class_=db_class)
    return factory()


# Try and instantiate "global" lcdb
try:
    db = db_from_config()
except KeyError:
    db = None
