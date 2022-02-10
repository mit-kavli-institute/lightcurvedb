import os
from datetime import datetime

TESS_FIRST_LIGHT = datetime(2018, 8, 7)
GPS_LEAP_SECONDS = 18
__DEFAULT_PATH__ = os.path.expanduser(
    os.path.join("~", ".config", "lightcurvedb", "db.conf")
)
TIC8_TEMPLATE = "{dialect}://{username}:{password}@{host}:{port}/{database}"
