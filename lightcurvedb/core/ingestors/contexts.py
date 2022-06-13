"""This module describes needed contexts/state that is needed across ingestion
processes.

The functions in this module create a sqlite3 database for the lifetime of one
ingestion invocation. Previous behavior required operators to maintain a
persistent database which resulted in errors and headache.

There is a bit more overhead with these methods. But overall the impact
is minimal.

"""
import sqlite3
from functools import wraps

import numpy as np
import pandas as pd

from lightcurvedb.models import Frame, SpacecraftEphemeris
from lightcurvedb.util.iter import chunkify


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
    """
    Yield quality flag cadence and flag values. The file should be
    whitespace delimited with the cadence as the first column.

    Note
    ----
    Currently quality flag file contents do not provide camera and ccd
    information. That must be determined by the developer. The constant
    variadic parameter allows passing in of constants for this purpose.
    """
    with open(quality_flag_path, "rt") as fin:
        for line in fin:
            cadence, quality_flag = line.strip().split()
            yield int(float(cadence)), int(float(quality_flag)), *constants


@with_sqlite
def populate_tic_catalog(conn, catalog_path, chunksize=1024):
    """
    Pull the tic catalog into the sqlite3 temporary database.

    Parameters
    ----------
    conn: pathlike
        A path to a sqlite3 database to push tic catalog information into.
    catalog_path: pathlike
        A path to the desired tic catalog.
    chunksize: int, optional
        The maximum rows inserted at once.
        Tic catalogs can be large, to avoid overwhelming sqlite3 with
        incredibly long query strings the population process is chunkified
        with length of this parameter.
    """
    with conn:
        for chunk in chunkify(_iter_tic_catalog(catalog_path), chunksize):
            conn.executemany(
                "INSERT OR IGNORE INTO tic_parameters("
                " tic_id, ra, dec, tmag, pmra, pmdec, jmag, kmag, vmag"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                chunk,
            )


@with_sqlite
def populate_quality_flags(conn, quality_flag_path, camera, ccd):
    """Populate the cache with the desired quality flags.

    Parameters
    ----------
    conn: pathlike
        A path to a sqlite3 database to push quality flag information into.
    quality_flag_path: pathlike
        A path to the desired quality flags.
    camera: int
        The camera of the quality flags.
    ccd: int
        The ccd of the quality flags.
    """
    with conn:
        _iter = _iter_quality_flags(quality_flag_path, camera, ccd)
        for chunk in chunkify(_iter, 1024):
            conn.executemany(
                "INSERT INTO quality_flags(cadence, quality_flag, camera, ccd)"
                " VALUES (?, ?, ?, ?)",
                chunk,
            )


@with_sqlite
def populate_ephemeris(conn, db):
    """Populate the cache with spacecraft ephemeris data.

    This method also requires a postgresql connection and will only
    represent a static snapshot of the state of the database during
    the initial phase of ingestion.

    Parameters
    ----------
    conn: pathlike
        A path to a sqlite3 database to push ephemeris data to.
    db: lightcurvedb.core.connection.ORMDB
        An open lcdb connection object to read from.
    """
    with conn:
        q = db.query(
            SpacecraftEphemeris.barycentric_dynamical_time,
            SpacecraftEphemeris.x,
            SpacecraftEphemeris.y,
            SpacecraftEphemeris.z,
        ).order_by(SpacecraftEphemeris.barycentric_dynamical_time)
        conn.executemany(
            "INSERT INTO spacecraft_pos(bjd, x, y, z) VALUES (?, ?, ?, ?)",
            q,
        )


@with_sqlite
def populate_tjd_mapping(conn, db, frame_type=None):
    """Populate the cache with tjd data.

    This method also requires a postgresql connection and will only
    represent a static snapshot of the state of the database during
    the initial phase of ingestion.

    Parameters
    ----------
    conn: pathlike
        A path to a sqlite3 database to push ephemeris data to.
    db: lightcurvedb.core.connection.ORMDB
        An open lcdb connection object to read from.
    """
    with conn:
        q = db.query(Frame.cadence, Frame.camera, Frame.mid_tjd).filter(
            Frame.frame_type_id
            == ("Raw FFI" if frame_type is None else frame_type)
        )
        conn.executemany(
            "INSERT INTO tjd_map(cadence, camera, tjd) VALUES (?, ?, ?)", q
        )


def _none_to_nan(value):
    return float("nan") if value is None else value


@with_sqlite
def get_tic_parameters(conn, tic_id, parameter, *parameters):
    """Grab the tic paramters for the specified tic_id

    Parameters
    ----------
    tic_id: int
        The tic id key.
    parameter: str
        The paramter of the tic id.
    *parameters: iterable
        Additional names of parameters to map.

    Returns
    -------
    dict
        A dictionary of parameter -> value respectively to the given tic id.

    Raises
    ------
    KeyError:
        The specified `tic_id` was not found in the sqlite3 cache.
    """
    param_order = (parameter, *parameters)
    param_str = ", ".join(param_order)
    q = conn.execute(
        f"SELECT {param_str} FROM tic_parameters WHERE tic_id = {tic_id}"
    )
    result = q.fetchall()[0]
    return dict(zip(param_order, map(_none_to_nan, result)))


@with_sqlite
def get_tic_mapping(conn, parameter, *parameters):
    """Map tic ids to the specified parameters.

    Parameters
    ----------
    conn: pathlike
        The path to a sqlite3 database to read tic parameters.
    parameter: str
        The parameter to be mapped to.
    *parameters: iterable
        Additional parameters be mapped.

    Returns
    -------
    list
        A list of dictionaries with all specified parameters as the
        keys.
    """
    param_order = ("tic_id", parameter, *parameters)
    param_str = ", ".join(param_order)
    q = conn.execute(f"SELECT {param_str} FROM tic_parameters")
    mapping = {}
    for tic_id, *parameters in q.fetchall():
        values = dict(zip(param_order[1:], map(_none_to_nan, parameters)))
        mapping[tic_id] = values
    return mapping


@with_sqlite
def get_qflag(conn, cadence, camera, ccd):
    """Get the quality flag at a certain camera, ccd, and cadence.

    Parameters
    ----------
    conn: pathlike
        The path to a sqlite3 database to read quality flags.
    cadence: int
        The cadence desired.
    camera: int
        The camera desired.
    ccd: int
        The CCD desired.

    Returns
    -------
    int:
        The quality flag.

    Raises
    -------
    ValueError
        Raised if the specified filters do not result in any quality flags.
    """
    q = conn.execute(
        "SELECT quality_flag FROM quality_flags WHERE "
        f"cadence = {cadence} AND camera = {camera} AND ccd = {ccd}"
    )
    results = q.fetchall()
    if len(results) == 0:
        raise ValueError(
            "Attempt to query for quality flag failed, "
            "no quality flags exist for specified parameters."
        )
    return results[0][0]


@with_sqlite
def get_qflag_np(conn, camera, ccd, cadence_min=None, cadence_max=None):
    """Get quality flags as structured numpy array with the keys of
    `cadence` and `quality_flag`.

    Parameters
    ----------
    conn: pathlike
        The path to a sqlite3 database to read quality flags.
    camera: int
        The camera to look for quality flags.
    ccd: int
        The CCD to look for quality flags.
    cadence_min: int, optional
        A lower cadence cutoff, if desired.
    cadence_max: int, optional
        A high cadence cutoff, if resired.

    Returns
    -------
    np.array
        A structured numpy array.
    """
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
        q.fetchall(), dtype=[("cadence", np.int32), ("quality_flag", np.int8)]
    )


@with_sqlite
def get_quality_flag_mapping(conn):
    """Construct a pandas dataframe of quality flags, indexed by camera, CCD,
    and cadence.

    Parameters
    ----------
    conn: pathlike
        The path to a sqlite3 database to read quality flags.

    Returns
    -------
    pd.DataFrame
        A pandas dataframe indexed by (camera, CCD, and cadence). The
        resulting dataframe is not sorted.
    """
    df = pd.read_sql(
        "SELECT camera, ccd, cadence, quality_flag FROM quality_flags "
        "ORDER BY camera, ccd, cadence",
        conn,
        index_col=["camera", "ccd", "cadence"],
    )
    return df


@with_sqlite
def get_spacecraft_data(conn, col):
    """Create a numpy array of values from spacecraft ephemeris data.
    The data is ordered by their corresponding barycentric julian date
    (ascending).

    Parameters
    ----------
    conn: pathlike
        The path to a sqlite3 database to read ephemeris data from.
    col: str
        The column to retrieve.

    Returns
    -------
    np.array
        A 1-Dimensional Numpy array containing values in the specified
        `col`. This field will be ordered by ascending barycentric julian
        date.
    """
    q = f"SELECT {col} FROM spacecraft_pos ORDER BY bjd"
    q = conn.execute(q)
    return np.array(list(val for val, in q))


@with_sqlite
def get_tjd_mapping(conn):
    """Create a pandas Dataframe of tjd values, indexed by camera and cadence.

    Parameters
    ----------
    conn: pathlike
        The path to a sqlite3 database to read tjd data from.

    Returns
    -------
    pd.DataFrame
        A dataframe indexed by camera and cadence to tjd values. This
        dataframe is not ordered and all NULL values will be forced into
        NaN values.
    """
    df = pd.read_sql(
        "SELECT camera, cadence, tjd FROM tjd_map ORDER BY camera, cadence",
        conn,
        index_col=["camera", "cadence"],
    ).fillna(value=np.nan)
    return df
