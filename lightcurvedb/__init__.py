__version__ = '0.1.11'


from lightcurvedb.core.engines import __SESSION_FACTORY__, __DEFAULT_PATH__
from lightcurvedb.core.base_model import QLPModel, QLPDataProduct, QLPDataSubType, QLPReference
from lightcurvedb.models import Aperture, FrameType, Frame, Orbit, Lightcurve, LightcurveType
from lightcurvedb.core.connection import DB, db_from_config
from lightcurvedb.managers import LightcurveManager


# Register global db instance if available
if __SESSION_FACTORY__ is not None:
    db = DB(__SESSION_FACTORY__)
    db._config = __DEFAULT_PATH__

# Avoid polluting namespace
del __SESSION_FACTORY__
del __DEFAULT_PATH__
