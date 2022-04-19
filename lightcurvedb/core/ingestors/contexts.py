import pandas as pd
import sqlite3
import numpy as np
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
        conn.execute(
            "CREATE TABLE tjd_map "
            "(cadence INTEGER,"
            " camera INTEGER,"
            " tjd REAL,"
            " PRIMARY KEY (cadence, camera))"
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


def _iter_quality_flags(quality_flag_path, *constants):
    with open(quality_flag_path, "rt") as fin:
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
                "INSERT INTO quality_flags(cadence, quality_flag, camera, ccd)"
                " VALUES (?, ?, ?, ?)",
                chunk
            )


@with_sqlite
def populate_ephemris(conn, db):
    with conn:
        q = (
            db
            .query(
               SpacecraftEphemris.barycentric_dynamical_time,
               SpacecraftEphemris.x,
               SpacecraftEphemris.y,
               SpacecraftEphemris.z
            )
            .order_by(SpacecraftEphemris.barycentric_dynamical_time)
        )
        conn.executemany(
            "INSERT INTO spacecraft_pos(bjd, x, y, z) VALUES (?, ?, ?, ?)",
            q.all()
        )


@with_sqlite
def populate_tjd_mapping(conn, db):
    with conn:
        q = (
            db
            .query(
                Frame.cadence,
                Frame.camera,
                Frame.mid_tjd
            )
            .filter(
                Frame.frame_type_id == "Raw FFI"
            )
        )
        conn.executemany(
            "INSERT INTO tjd_map(cadence, camera, tjd) VALUES (?, ?, ?)",
            q
        )


def _none_to_nan(value):
    return float("nan") if value is None else value


@with_sqlite
def get_tic_parameters(conn, tic_id, *parameters):
    param_str = ", ".join(parameters)
    q = conn.execute(
        f"SELECT {param_str} FROM tic_parameters WHERE tic_id = {tic_id}"
    )
    result = q.fetchall()[0]
    return dict(zip(parameters, map(_none_to_nan, result)))


@with_sqlite
def get_tic_mapping(conn, *columns):
    param_str = ", ".join(("tic_id", *columns))
    q = conn.execute(
        f"SELECT {param_str} FROM tic_parameters"
    )
    mapping = {}
    for tic_id, *parameters in q.fetchall():
        values = dict(zip(columns, map(_none_to_nan, parameters)))
        mapping[tic_id] = values
    return mapping


@with_sqlite
def get_qflag(conn, cadence, camera, ccd):
    q = conn.execute(
        "SELECT quality_flag FROM quality_flags WHERE "
        f"cadence = {cadence} AND camera = {camera} AND ccd = {ccd}"
    )
    return q.fetchall()[0][0]


@with_sqlite
def get_qflag_np(conn, camera, ccd, cadence_min=None, cadence_max=None):
    base_q = "SELECT cadence, quality_flag FROM quality_flags "
    filter_q = f"WHERE camera = {camera} AND ccd = {ccd}"
    if cadence_min is not None and cadence_max is not None:
        filter_q += f" AND cadence BETWEEN {cadence_min} AND {cadence_max}"
    elif cadence_min is not None:
        filter_q += f" AND cadence >= {cadence_min}"
    elif cadence_max is not None:
        filter_q += f" AND cadence <= {cadence_max}"

    q = conn.execute(base_q + filter_q + " ORDER BY cadence")
    
    return np.array(
        q.fetchall(),
        dtype=[("cadence", np.int32), ("quality_flag", np.int8)]
    )

@with_sqlite
def get_quality_flag_mapping(conn):
    df = pd.read_sql(
        "SELECT camera, ccd, cadence, quality_flag FROM quality_flags ORDER BY camera, ccd, cadence",
        conn,
        index_col=["camera", "ccd", "cadence"]
    )
    return df


@with_sqlite
def get_spacecraft_data(conn, col):
    q = f"SELECT {col} FROM spacecraft_pos ORDER BY bjd"
    q = conn.execute(q)
    return np.array(list(val for val, in q))


@with_sqlite
def get_tjd_mapping(conn):
    df = (
        pd
        .read_sql(
            "SELECT camera, cadence, tjd FROM tjd_map ORDER BY camera, cadence",
            conn,
            index_col=["camera", "cadence"]
        )
        .fillna(value=np.nan)
    )
    return df
