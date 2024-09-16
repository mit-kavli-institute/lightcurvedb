__version__ = "0.16.0"

from lightcurvedb.core.connection import db, db_from_config

__all__ = [
    "db_from_config",
    "db",
]
