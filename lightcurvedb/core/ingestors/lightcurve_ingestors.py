import os
import re
import warnings

import numpy as np
import pandas as pd
from sqlalchemy import text
from lightcurvedb.models import Lightcurve, Frame
from lightcurvedb.core.connection import db_from_config
from lightcurvedb.core.ingestors.cache import IngestionCache
from lightcurvedb.core.ingestors.temp_table import QualityFlags
from functools import lru_cache
from lightcurvedb.util.decorators import track_runtime

LC_ERROR_TYPES = {"RawMagnitude"}

with warnings.catch_warnings():
    warnings.simplefilter("ignore", category=FutureWarning)
    from h5py import File as H5File


path_components = re.compile(
    (
        r"orbit-(?P<orbit>[1-9][0-9]*)/"
        r"ffi/"
        r"cam(?P<camera>[1-4])/"
        r"ccd(?P<ccd>[1-4])/"
        r"LC/"
        r"(?P<tic>[1-9][0-9]*)"
        r"\.h5$"
    )
)


def get_components(path):
    result = path_components.search(path)
    if not result:
        raise RuntimeError(
            "{0} does not look like a full H5 path.".format(path)
        )
    components = result.groupdict()
    return {
        "tic_id": int(components["tic"]),
        "orbit_number": int(components["orbit"]),
        "camera": int(components["camera"]),
        "ccd": int(components["ccd"]),
    }


@lru_cache(maxsize=32)
def get_qflags(min_cadence, max_cadence, camera, ccd):
    cache = IngestionCache()
    q = (
        cache
        .query(
            QualityFlags.cadence,
            QualityFlags.quality_flag
        )
        .filter(
            QualityFlags.cadence.between(int(min_cadence), int(max_cadence)),
            QualityFlags.camera == camera,
            QualityFlags.ccd == ccd,
        )
    )
    return pd.read_sql(
        q.statement,
        cache.session.bind,
        index_col=["cadence"]
    )

@lru_cache(maxsize=32)
def get_mid_tjd(min_cadence, max_cadence, camera, config_override=None):
    with db_from_config(config_path=config_override) as db:
        q = (
            db
            .query(
                Frame.cadence,
                Frame.mid_tjd
            )
            .filter(
                Frame.frame_type_id == "Raw FFI",
                Frame.camera == camera,
                Frame.cadence.between(int(min_cadence), int(max_cadence))
            )
            .distinct(
                Frame.cadence
            )
        )
        df = pd.read_sql(
            q.statement,
            db.bind,
            index_col=["cadence"]
        )
    return df

@lru_cache(maxsize=10)
def get_h5(path):
    return H5File(path, "r")


@track_runtime
def get_h5_data(merge_job):
    lc = get_h5(merge_job.file_path)
    lc = lc["LightCurve"]
    cadences = lc["Cadence"][()].astype(int)

    lc = lc["AperturePhotometry"][merge_job.aperture]
    x_centroids = lc["X"][()]
    y_centroids = lc["Y"][()]
    data = lc[merge_job.lightcurve_type][()]
    errors = (
        lc["{0}Error".format(merge_job.lightcurve_type)][()]
        if merge_job.lightcurve_type in LC_ERROR_TYPES
        else np.full_like(cadences, np.nan, dtype=np.double)
    )
    return {
        "cadence": cadences,
        "data": data,
        "error": errors,
        "x_centroid": x_centroids,
        "y_centroid": y_centroids
    }


@track_runtime
def get_correct_qflags(merge_job, cadences):
    """
    Grab the user-assigned quality flags from cache usiing a single merge job
    as a filter as well as a list of reference cadences.

    Returns
    -------
    np.ndarray
        A numpy array of quality flag integers in order of the provided
        cadences.
    """
    min_c, max_c = min(cadences), max(cadences)
    qflag_df = get_qflags(min_c, max_c, merge_job.camera, merge_job.ccd)
    return qflag_df.loc[cadences]["quality_flag"].to_numpy()


@track_runtime
def get_tjd(merge_job, cadences, config_override=None):
    min_c, max_c = min(cadences), max(cadences)
    tjd_df = get_mid_tjd(
        min_c,
        max_c,
        merge_job.camera,
        config_override=config_override
    )
    return tjd_df.reindex(cadences)["mid_tjd"].to_numpy()

@track_runtime
def get_lightcurve_median(data, quality_flags, tmag):
    mask = quality_flags == 0
    offset = np.nanmedian(data[mask]) - tmag
    return data - offset


def get_missing_ids(db, max_return=None):
    """
    Return missing lightcurve ids from the database. This method is
    not multiprocess safe. And the results returned are true only if
    no other processes are adding ids.

    Parameters
    ----------
    db: lightcurvedb.core.connection.DB
        An open database connection.
    max_return: int, optional
        The maximum number of ids to return. If this limit is reached
        then the returned id set length will == ``max_return``.
    Returns
    -------
    set
        A set of integers.
    """
    id_q = db.query(Lightcurve.id)
    ids = {id_ for id_, in id_q}

    max_id = max(ids)
    ref_ids = set(range(1, max_id + 1))

    missing = ref_ids - ids
    if max_return:
        return set(sorted(missing)[:max_return])
    return missing


def allocate_lightcurve_ids(db, n_ids):
    """
    Allocates ``n_ids`` ids from the database.
    """
    if n_ids <= 0:
        return []

    q = text(
        "SELECT nextval('{0}') "
        "FROM generate_series(1, {1})".format("lightcurves_id_seq", n_ids)
    )
    return [id_ for id_, in db.session.execute(q)]
