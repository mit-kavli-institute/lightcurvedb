__version__ = "0.8.5"

from lightcurvedb.core.connection import DB, db_from_config
from lightcurvedb.core.engines import __DEFAULT_PATH__, __SESSION_FACTORY__
from lightcurvedb.managers import LightcurveManager
from lightcurvedb.models import (
    Aperture,
    Lightcurve,
    LightcurveType,
    Lightpoint,
    Observation,
    Orbit,
)

__all__ = [
    "Aperture",
    "Lightcurve",
    "LightcurveManager",
    "LightcurveType",
    "Lightpoint",
    "Observation",
    "Orbit",
    "db_from_config",
]


# Register global db instance if available
if __SESSION_FACTORY__ is not None:
    db = DB(__SESSION_FACTORY__)
    db._config = __DEFAULT_PATH__
else:
    db = None

# Avoid polluting namespace
del __SESSION_FACTORY__
del __DEFAULT_PATH__
