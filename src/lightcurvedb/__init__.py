__version__ = "0.0.0"  # Managed by python-semantic-release

from lightcurvedb.core.connection import db, db_from_config

__all__ = [
    "db_from_config",
    "db",
]
