"""This module describes needed contexts/state that is needed across ingestion
processes.

The functions in this module create a sqlite3 database for the lifetime of one
ingestion invocation. Previous behavior required operators to maintain a
persistent database which resulted in errors and headache.

There is a bit more overhead with these methods. But overall the impact
is minimal.

"""
import pathlib
from functools import wraps

import numpy as np
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import Session, as_declarative, declared_attr
from tqdm import tqdm

from lightcurvedb.models import Frame, FrameType, SpacecraftEphemeris
from lightcurvedb.util.iter import chunkify

MAX_PARAM = 999


@as_declarative()
class ContextBase:
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower() + "s"

    @classmethod
    def dynamic_select(cls, *fields):
        cols = tuple(getattr(cls, field) for field in fields)
        return sa.select(*cols)

    @classmethod
    def insert(cls, *args, **kwargs):
        return sa.insert(cls, *args, **kwargs)

    @classmethod
    def select(cls, *args, **kwargs):
        return sa.select(cls, *args, **kwargs)


class TicParameter(ContextBase):
    tic_id = sa.Column(sa.BigInteger, primary_key=True)
    ra = sa.Column(sa.Float)
    dec = sa.Column(sa.Float)
    tmag = sa.Column(sa.Float)
    pmra = sa.Column(sa.Float)
    pmdec = sa.Column(sa.Float)
    jmag = sa.Column(sa.Float)
    kmag = sa.Column(sa.Float)
    vmag = sa.Column(sa.Float)


class QualityFlag(ContextBase):
    cadence = sa.Column(sa.Integer, primary_key=True)
    camera = sa.Column(sa.SmallInteger, primary_key=True)
    ccd = sa.Column(sa.SmallInteger, primary_key=True)
    quality_flag = sa.Column(sa.Integer)


class SpacecraftPosition(ContextBase):
    bjd = sa.Column(sa.Float, primary_key=True)
    x = sa.Column(sa.Float)
    y = sa.Column(sa.Float)
    z = sa.Column(sa.Float)


class TJDMapping(ContextBase):
    cadence = sa.Column(sa.Integer, primary_key=True)
    camera = sa.Column(sa.SmallInteger, primary_key=True)
    tjd = sa.Column(sa.Float)

    @hybrid_property
    def mid_tjd(self):
        # Keyword compatibility with Frame
        return self.tjd

    @mid_tjd.expression
    def mid_tjd(cls):
        # Keyword compatibility with Frame
        return cls.tjd


class LightcurveIDMapping(ContextBase):
    id = sa.Column(sa.BigInteger, primary_key=True)
    tic_id = sa.Column(sa.BigInteger)
    camera = sa.Column(sa.SmallInteger)
    ccd = sa.Column(sa.SmallInteger)
    orbit_id = sa.Column(sa.SmallInteger)
    aperture_id = sa.Column(sa.SmallInteger)
    lightcurve_type_id = sa.Column(sa.Integer)


def with_sqlite(function):
    """
    Utility decorator for providing an sqlite3 connection inside the
    decorated function. Developers are expected to commit/rollback any
    changes within their function. The decorator does not call commits or
    rollbacks or give the connection within a contextual manager.
    """

    @wraps(function)
    def wrapper(db_path, *args, **kwargs):
        path = pathlib.Path(db_path)
        url = f"sqlite:///{path}"
        engine = sa.create_engine(url)
        with Session(engine) as session:
            return function(session, *args, **kwargs)

    return wrapper


@with_sqlite
def make_shared_context(session):
    """
    Creates the expected tables needed for ingestion contexts.
    """
    ContextBase.metadata.create_all(bind=session.bind)


def _iter_tic_catalog(catalog_path, mask, field_order=None):
    """
    Yield paramters within the catalog path. The file should be whitespace
    delimited with the TIC_ID parameter as the first column.
    """
    if field_order is None:
        field_order = (
            "tic_id",
            "ra",
            "dec",
            "tmag",
            "pmra",
            "pmdec",
            "jmag",
            "kmag",
            "vmag",
        )
    with open(catalog_path, "rt") as fin:
        for line in fin:
            tic_id, *data = line.strip().split()
            tic_id = int(tic_id)
            if tic_id in mask:
                continue
            fields = (tic_id, *data)
            yield dict(zip(field_order, fields))
            mask.add(tic_id)


def _iter_quality_flags(quality_flag_path, camera, ccd):
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
            yield int(float(cadence)), camera, ccd, int(float(quality_flag))


@with_sqlite
def populate_tic_catalog(conn, catalog_path, chunksize=MAX_PARAM):
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
    mask = {tic_id for tic_id, in conn.query(TicParameter.tic_id)}
    parameters = list(_iter_tic_catalog(catalog_path, mask))
    chunks = chunkify(tqdm(parameters, unit=" tics"), chunksize // 9)

    for chunk in chunks:
        stmt = TicParameter.insert().values(chunk)
        conn.execute(stmt)
        conn.commit()


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
    _iter = _iter_quality_flags(quality_flag_path, camera, ccd)
    parameters = list(_iter)
    for chunk in chunkify(tqdm(parameters, unit=" qflags"), MAX_PARAM // 4):
        stmt = QualityFlag.insert().values(chunk)
        conn.execute(stmt)
        conn.commit()


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
    cols = (
        "bjd",
        "x",
        "y",
        "z",
    )
    q = db.query(
        *tuple(getattr(SpacecraftEphemeris, col) for col in cols)
    ).order_by(SpacecraftEphemeris.barycentric_dynamical_time)

    chunks = chunkify(tqdm(q.all(), unit=" positions"), MAX_PARAM // len(cols))

    for chunk in chunks:
        stmt = SpacecraftPosition.insert().values(chunk)
        conn.execute(stmt)
    conn.commit()


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
    cols = ("cadence", "camera", "tjd")
    type_name = "Raw FFI" if frame_type is None else frame_type
    q = (
        db.query(Frame.cadence, Frame.camera, Frame.mid_tjd)
        .join(FrameType, FrameType.name == Frame.frame_type_id)
        .filter(FrameType.name == type_name)
    )
    payload = [dict(zip(cols, row)) for row in q]
    if len(payload) == 0:
        raise RuntimeError(
            "Unable to find any TJD values from frame query using: "
            f"frame type name: {type_name}"
        )
    chunksize = MAX_PARAM // len(cols)
    for chunk in chunkify(tqdm(payload, unit=" tjds"), chunksize):
        stmt = TJDMapping.insert().values(chunk)
        conn.execute(stmt)
    conn.commit()


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
    stmt = TicParameter.dynamic_select(*param_order).filter_by(tic_id=tic_id)
    return dict(zip(param_order, map(_none_to_nan, conn.execute(stmt).one())))


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
    stmt = TicParameter.dynamic_select(*param_order)
    mapping = {}
    for tic_id, *parameters in conn.execute(stmt).fetchall():
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
    stmt = QualityFlag.dynamic_select("quality_flag").filter_by(
        cadence=cadence, camera=camera, ccd=ccd
    )
    results = conn.execute(stmt).fetchall()
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
        A lower cadence cutoff (inclusive), if desired.
    cadence_max: int, optional
        A high cadence cutoff (inclusive), if resired.

    Returns
    -------
    np.array
        A structured numpy array.
    """
    filters = [
        QualityFlag.camera == camera,
        QualityFlag.ccd == ccd,
    ]
    if cadence_min is not None and cadence_max is not None:
        filter.append(QualityFlag.cadence.between(cadence_min, cadence_max))
    elif cadence_min is not None:
        filter.append(QualityFlag.cadence >= cadence_min)
    elif cadence_max is not None:
        filter.append(QualityFlag.cadence <= cadence_max)
    stmt = (
        QualityFlag.dynamic_select("cadence", "quality_flag")
        .where(*filters)
        .order_by(QualityFlag.cadence)
    )

    df = pd.read_sql(stmt, conn.bind)
    return np.array(
        df.to_records(index=False),
        dtype=[("cadence", np.int32), ("quality_flag", np.int8)],
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
    stmt = QualityFlag.dynamic_select(
        "camera", "ccd", "cadence", "quality_flag"
    ).order_by(QualityFlag.camera, QualityFlag.ccd, QualityFlag.cadence)

    df = pd.read_sql(
        stmt,
        conn.bind,
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
    stmt = SpacecraftPosition.dynamic_select(col).order_by(
        SpacecraftPosition.bjd
    )
    q = conn.execute(stmt)
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
    stmt = TJDMapping.dynamic_select("cadence", "camera", "tjd").order_by(
        TJDMapping.camera, TJDMapping.cadence
    )
    df = pd.read_sql(
        stmt,
        conn.bind,
        index_col=["camera", "cadence"],
    ).fillna(value=np.nan)
    return df
