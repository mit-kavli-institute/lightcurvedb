__version__ = "0.15.1"

from lightcurvedb.core.connection import db, db_from_config

__all__ = [
    "db_from_config",
    "db",
]
