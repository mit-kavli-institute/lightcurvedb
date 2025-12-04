__version__ = "2.2.1-beta.1"  # Managed by python-semantic-release

from lightcurvedb.core.connection import LCDB_Session, db, db_from_config

__all__ = [
    "db_from_config",
    "db",
    "LCDB_Session",
]
