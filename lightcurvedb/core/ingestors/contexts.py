import pandas as pd
import sqlite3
from lightcurvedb.util.iter import chunkify
from lightcurvedb.models import Frame, SpacecraftEphemris
from functools import wraps


def with_sqlite(function):
    """
    Utility decorator for providing an sqlite3 connection inside the
    decorated function. Developers are expected to commit/rollback any
    changes within their function. The decorator does not call commits or
    rollbacks or give the connection within a contextual manager.
    """
    @wraps(function)
    def wrapper(db_path, *args, **kwargs):
        conn = sqlite3.connect(db_path)
        return function(conn, *args, **kwargs)
    return wrapper


@with_sqlite
def make_shared_context(conn):
    """
    Creates the expected tables needed for ingestion contexts.
    """
    # Establish tables
    with conn:
        conn.execute(
            "CREATE TABLE tic_parameters "
            "(tic_id integer PRIMARY KEY,"
            " ra REAL,"
            " dec REAL,"
            " tmag REAL,"
            " pmra REAL,"
            " pmdec REAL,"
            " jmag REAL,"
            " kmag REAL,"
            " vmag REAL)"
        )
        conn.execute(
            "CREATE TABLE quality_flags "
            "(cadence INTEGER,"
            " camera INTEGER,"
            " ccd INTEGER,"
            " quality_flag INTEGER,"
            " PRIMARY KEY (cadence, camera, ccd))"
        )
        conn.execute(
            "CREATE TABLE spacecraft_pos "
            "(bjd REAL PRIMARY KEY,"
            " x REAL,"
            " y REAL,"
            " z REAL)"
        )
    # Tables have been made


def _iter_tic_catalog(catalog_path):
    """
    Yield paramters within the catalog path. The file should be whitespace
    delimited with the TIC_ID parameter as the first column.
    """
    with open(catalog_path, "rt") as fin:
        for line in fin:
            tic_id, *data = line.strip().split()
            yield int(tic_id), *tuple(map(float, data))


def __iter_quality_flags(quality_flag_path, *constants):
    with open(catalog_path, "rt") as fin:
        for line in fin:
            cadence, quality_flag = line.strip().split()
            yield int(cadence), int(quality_flag), *constants


@with_sqlite
def populate_tic_catalog(conn, catalog_path, chunksize=1024, sort=False):
    """
    Pull the tic catalog into the sqlite3 temporary database.
    """
    with conn:
        for chunk in chunkify(_iter_tic_catalog(catalog_path), chunksize):
            conn.executemany(
                "INSERT INTO tic_parameters("
                " tic_id, ra, dec, tmag, pmra, pmdec, jmag, kmag, vmag"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                chunk
            )

@with_sqlite
def populate_quality_flags(conn, quality_flag_path, camera, ccd):
    with conn:
        _iter = _iter_quality_flags(quality_flag_path, camera, ccd)
        for chunk in chunkify(_iter, 1024):
            conn.executemany(
                "INSERT INTO quality_flags(cadence, quality_flag)"
                " VALUES (?, ?)",
                chunk
            )


@with_sqlite
def populate_ephemeris(conn, db):
    with conn:
        q = (
            db
            .query(
               Spacecraft.barycentric_dynamical_time,
               Spacecraft.x,
               Spacecraft.y,
               Spacecraft.z
            )
            .order_by(Spacecraft.barycentric_dynamical_time)
        )
        conn.executemany(
            "INSERT INTO spacecraft_pos(bjd, x, y, z) VALUES (?, ?, ?, ?)",
            q.all()
        )


@with_sqlite
def get_tic_parameters(conn, tic_id, *parameters):
    param_str = ", ".join(paramters)
    q = conn.execute(
        f"SELECT {param_str} FROM tic_parameters WHERE tic_id = {tic_id}"
    )
    result = q.fetchall()[0]
    return dict(zip(parameters, result))
