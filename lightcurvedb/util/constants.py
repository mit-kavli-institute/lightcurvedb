import pathlib
from datetime import datetime

TESS_FIRST_LIGHT = datetime(2018, 8, 7)
GPS_LEAP_SECONDS = 18
__DEFAULT_PATH__ = pathlib.Path(
    "~", ".config", "lightcurvedb", "db.conf"
).expanduser()
TIC8_TEMPLATE = "{dialect}://{username}:{password}@{host}:{port}/{database}"
