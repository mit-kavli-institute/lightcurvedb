import pathlib

DEFAULT_CONFIG_PATH = pathlib.Path(
    "~", ".config", "lightcurvedb", "db.conf"
).expanduser()
