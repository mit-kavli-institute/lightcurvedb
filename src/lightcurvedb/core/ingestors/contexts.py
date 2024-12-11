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
from loguru import logger
from sqlalchemy.orm import Session, as_declarative, declared_attr
from tqdm import tqdm

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


class QualityFlag(ContextBase):
    cadence = sa.Column(sa.Integer, primary_key=True)
    camera = sa.Column(sa.SmallInteger, primary_key=True)
    ccd = sa.Column(sa.SmallInteger, primary_key=True)
    quality_flag = sa.Column(sa.Integer)


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
        url = f"sqlite:///{path}"  # noqa
        engine = sa.create_engine(url)
        with Session(engine) as session:
            return function(session, *args, **kwargs)

    return wrapper


@with_sqlite
def make_shared_context(session):
    """
    Creates the expected tables needed for ingestion contexts.
    """
    logger.debug("Creating SQLite Cache")
    ContextBase.metadata.create_all(bind=session.bind)


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
    logger.debug(f"Loading {quality_flag_path}")
    _iter = _iter_quality_flags(quality_flag_path, camera, ccd)
    parameters = list(_iter)
    for chunk in chunkify(tqdm(parameters, unit=" qflags"), MAX_PARAM // 4):
        stmt = QualityFlag.insert().values(chunk)
        conn.execute(stmt)
        conn.commit()


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
        filters.append(QualityFlag.cadence.between(cadence_min, cadence_max))
    elif cadence_min is not None:
        filters.append(QualityFlag.cadence >= cadence_min)
    elif cadence_max is not None:
        filters.append(QualityFlag.cadence <= cadence_max)
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
