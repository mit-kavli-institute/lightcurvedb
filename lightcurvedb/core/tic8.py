import os
import sys
import warnings
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SAWarning
from sqlalchemy import create_engine, Table
from lightcurvedb.core.engines import engine_from_config
from lightcurvedb.core.connection import ORM_DB
from lightcurvedb.util.constants import __DEFAULT_PATH__



class TIC8_DB(ORM_DB):
    def __init__(self, config_path=None, config_section=None, greedy_reflect=True):
        """
        Initializes a connection to the TIC8 Database.

        Parameters
        ----------
        config_path: str or path-like
            The path to the INI-like configuration file that holds the
            credentials to the relevant TIC8 database.
        config_section: str
            The configuration section to read in the INI file.
        greedy_reflect: bool, optional
            If true a connection is immediately established to reflect
            the remote schemas to construct Python-side objects. If false
            this will occur with the first query.
        """
        path = os.path.expanduser(config_path if config_path else __DEFAULT_PATH__)
        section = config_section if config_section else "TIC8 Credentials"
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=SAWarning)

                engine = engine_from_config(
                    path,
                    config_group=section,
                    uri_template="{dialect}://{username}:{password}@{host}:{port}/{database}"
                )
                self.engine = engine
                self._sessionmaker = sessionmaker(autoflush=True)
                self._sessionmaker.configure(bind=engine)
                self._session_stack = []
                self._config = path
                self._max_depth = 10

                if greedy_reflect:
                    self.__reflect__()
        except IOError:
            sys.stderr.write(
                (
                    "{0} was not found, "
                    "please check your configuration environment\n".format(
                        CONFIG_PATH
                    )
                )
            )
            self.TIC8_Base = None
            self.engine = None
            self.entries = None

    def __reflect__(self):
        BASE = automap_base()
        BASE.prepare(self.engine, reflect=True)
        self.TIC8_Base = BASE
        self.data_class = Table(
            "ticentries", BASE.metadata, autoload=True, autoload_with=self.engine
        )

    @property
    def ticentries(self):
        if self.TIC8_Base is None:
            self.__reflect__(self.engine)
        return self.data_class

    def get_stellar_param(self, tic_id, *parameters):
        cols = [self.ticentries.c[column] for column in parameters]
        return self.query(*cols).filter(self.ticentries.c.id == tic_id).one()

    def mass_stellar_param_q(self, tic_ids, *parameters):
        cols = [self.ticentries.c[column] for column in parameters]
        return (
            self.query(*cols).filter(self.ticentries.c.id.in_(tic_ids)).all()
        )


def one_off(tic_id, *parameters):
    conn = TIC8_DB()

    with conn as tic8:
        result = tic8.get_stellar_param(tic_id, *parameters)
    return result
