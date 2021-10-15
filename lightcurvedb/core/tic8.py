import os
import sys
import warnings
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.exc import DisconnectionError, SAWarning
from sqlalchemy.event import listens_for
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
from sqlalchemy import create_engine, Table

CONFIG_PATH = os.path.expanduser(
    os.path.join("~", ".config", "tsig", "tic-dbinfo")
)

TIC8_CONFIGURATION = {
    "executemany_mode": "values",
    "executemany_values_page_size": 10000,
    "executemany_batch_page_size": 500,
}


class TIC8_DB(object):
    def __init__(self, config_path=CONFIG_PATH, greedy_reflect=True):
        """
        Initializes a connection to the TIC8 Database.

        Parameters
        ----------
        config_path: str or path-like
            The path to the INI-like configuration file that holds the
            credentials to the relevant TIC8 database.
        greedy_reflect: bool, optional
            If true a connection is immediately established to reflect
            the remote schemas to construct Python-side objects. If false
            this will occur with the first query.
        """
        self.session = None
        try:
            for line in open(config_path, "rt").readlines():
                key, value = line.strip().split("=")
                key = key.strip()
                value = value.strip()
                TIC8_CONFIGURATION[key] = value

            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=SAWarning)

                TIC8_ENGINE = create_engine(
                    "postgresql://{dbuser}:{dbpass}@{dbhost}:{dbport}/{dbname}".format(
                        **TIC8_CONFIGURATION,
                        poolclass=NullPool
                    )
                )

                @listens_for(TIC8_ENGINE, "connect")
                def connect(dbapi_connection, connection_record):
                    connection_record.info["pid"] = os.getpid()

                @listens_for(TIC8_ENGINE, "checkout")
                def checkout(
                    dbapi_connection, connection_record, connection_proxy
                ):
                    pid = os.getpid()
                    if connection_record.info["pid"] != pid:
                        connection_record.connection = None
                        connection_proxy.connection = None
                        raise DisconnectionError(
                            "Attempting to disassociate database connection"
                        )

                self.engine = TIC8_ENGINE
                if greedy_reflect:
                    self.__reflect__(TIC8_ENGINE)
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

    def __reflect__(self, engine):
        BASE = automap_base()
        BASE.prepare(engine, reflect=True)
        self.TIC8_Base = BASE
        self.sessionclass = sessionmaker(autoflush=True)
        self.sessionclass.configure(bind=engine)
        self.data_class = Table(
            "ticentries", BASE.metadata, autoload=True, autoload_with=engine
        )

    @property
    def ticentries(self):
        if self.TIC8_Base is None:
            self.__reflect__(self.engine)
        return self.data_class

    @property
    def is_active(self):
        return self.session is not None

    def __enter__(self):
        return self.open()

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def open(self):  # noqa: B006
        """
        Establish a connection to the TIC8 database. If this session
        has already been opened a warning will be emitted before a no-op.

        Returns
        -------
        TIC8_DB
            Returns itself in an open state
        """
        if not self.session:
            self.session = self.sessionclass()
        else:
            warnings.warn(
                "TIC8 Session has already been scoped. Ignoring duplicate "
                "open call",
                RuntimeWarning,
            )
        return self

    def close(self):
        """
        Closes the database connection. If this session has not been opened "
        "a warning will be emitted."
        """
        if self.session is not None:
            self.session.close()
            self.session = None
        else:
            warnings.warn(
                "TIC8 Session has already been closed. Ignoring duplicate "
                "close call.",
                RuntimeWarning,
            )
        return self

    def query(self, *args, **params):
        if not self.is_active:
            raise RuntimeError(
                "Session is not open. Please call `db_inst.open()` "
                "or use `with db_inst as opendb:`"
            )
        return self.session.query(*args, **params)

    def bind(self, *args, **params):
        if not self.is_active:
            raise RuntimeError(
                "Session is not open. Please call `db_inst.open()` "
                "or use `with db_inst as opendb:`"
            )
        return self.session.bind

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
