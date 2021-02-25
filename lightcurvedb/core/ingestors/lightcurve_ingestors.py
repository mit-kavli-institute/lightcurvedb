import os
import re
import warnings

import numpy as np
import pandas as pd
from lightcurvedb.models import (
    Lightcurve,
    Observation,
    Orbit,
    Aperture,
    LightcurveType,
)
from lightcurvedb.core.ingestors.temp_table import FileObservation
from sqlalchemy import text, distinct
from functools import lru_cache


with warnings.catch_warnings():
    warnings.simplefilter("ignore", category=FutureWarning)
    from h5py import File as H5File


THRESHOLD = 1 * 10 ** 9 / 4  # bytes


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


def quality_flag_extr(qflags):
    accept = np.ones(qflags.shape[0], dtype=np.int64)
    for i in range(qflags.shape[0]):
        if qflags[i] == b"G":
            accept[i] = 1
        else:
            accept[i] = 0
    return accept


# Def: KEY -> Has error field
H5_LC_TYPES = {"KSPMagnitude": False, "RawMagnitude": True}


def h5_to_kwargs(filepath, **constants):
    with H5File(filepath, "r") as h5in:
        lc = h5in["LightCurve"]
        tic = int(os.path.basename(filepath).split(".")[0])
        cadences = lc["Cadence"][()].astype(int)
        bjd = lc["BJD"][()]
        apertures = lc["AperturePhotometry"].keys()

        for aperture in apertures:
            lc_by_ap = lc["AperturePhotometry"][aperture]
            x_centroids = lc_by_ap["X"][()]
            y_centroids = lc_by_ap["Y"][()]
            quality_flags = quality_flag_extr(
                lc_by_ap["QualityFlag"][()]
            ).astype(int)

            for lc_type, has_error_field in H5_LC_TYPES.items():
                if lc_type not in lc_by_ap:
                    continue
                values = lc_by_ap[lc_type][()]
                if has_error_field:
                    errors = lc_by_ap["{0}Error".format(lc_type)][()]
                else:
                    errors = np.full_like(cadences, np.nan, dtype=np.double)

                yield dict(
                    tic_id=tic,
                    lightcurve_type_id=lc_type,
                    aperture_id=aperture,
                    cadences=cadences,
                    barycentric_julian_date=bjd,
                    values=values,
                    errors=errors,
                    x_centroids=x_centroids,
                    y_centroids=y_centroids,
                    quality_flags=quality_flags,
                    **constants
                )


def kwargs_to_df(*kwargs, **constants):
    dfs = []
    keys = [
        "cadences",
        "barycentric_julian_date",
        "values",
        "errors",
        "x_centroids",
        "y_centroids",
        "quality_flags",
    ]

    for kwarg in kwargs:
        df = pd.DataFrame(data={k: kwarg[k] for k in keys})
        df["lightcurve_id"] = kwarg["id"]
        df = df.set_index(["lightcurve_id", "cadences"])
        dfs.append(df)
    main = pd.concat(dfs)
    for k, constant in constants.items():
        main[k] = constant
    return main


def parse_h5(h5in, constants, lightcurve_id, aperture, type_):
    lc = h5in["LightCurve"]
    cadences = lc["Cadence"][()].astype(int)
    bjd = lc["BJD"][()]

    lc = lc["AperturePhotometry"][aperture]
    x_centroids = lc["X"][()]
    y_centroids = lc["Y"][()]
    quality_flags = quality_flag_extr(lc["QualityFlag"][()]).astype(int)

    values = lc[type_][()]
    has_error_field = H5_LC_TYPES[type_]

    if has_error_field:
        errors = lc["{0}Error".format(type_)][()]
    else:
        errors = np.full_like(cadences, np.nan, dtype=np.double)

    lightpoints = pd.DataFrame(
        data={
            "cadence": cadences,
            "barycentric_julian_date": bjd,
            "data": values,
            "error": errors,
            "x_centroid": x_centroids,
            "y_centroid": y_centroids,
            "quality_flag": quality_flags,
        }
    )
    lightpoints["lightcurve_id"] = lightcurve_id
    for fieldname, constant in constants.items():
        lightpoints[fieldname] = constant
    return lightpoints


def get_ingestion_plan(
    db,
    cache,
    orbits=None,
    cameras=None,
    ccds=None,
    tic_mask=None,
    invert_mask=False,
):

    cache_subquery = cache.query(distinct(FileObservation.tic_id))
    db_subquery = db.query(distinct(Observation.lightcurve_id))

    if orbits:
        db_subquery = db_subquery.filter(Orbit.orbit_number.in_(orbits))
        cache_subquery = cache_subquery.filter(
            FileObservation.orbit_number.in_(orbits)
        )

    if cameras:
        db_subquery = db_subquery.filter(Observation.camera.in_(cameras))
        cache_subquery = cache_subquery.filter(
            FileObservation.camera.in_(cameras)
        )

    if ccds:
        db_subquery = db_subquery.filter(Observation.ccd.in_(ccds))
        cache_subquery = cache_subquery.filter(FileObservation.ccd.in_(ccds))

    if tic_mask:
        db_tic_filter = (
            ~Observation.tic_id.in_(tic_mask)
            if invert_mask
            else Observation.tic_id.in_(tic_mask)
        )
        cache_tic_filter = (
            ~FileObservation.tic_id.in_(tic_mask)
            if invert_mask
            else FileObservation.tic_id.in_(tic_mask)
        )
        db_subquery = db_subquery.filter(db_tic_filter)
        cache_subquery = cache_subquery.filter(cache_tic_filter)

    seen_cache = set(
        db.query(Observation.lightcurve_id, Orbit.orbit_number)
        .join(Observation.orbit)
        .filter(Observation.lightcurve_id.in_(db_subquery.subquery()))
    )

    relevant_ids = set(row[0] for row in seen_cache)
    id_map = {
        (lc.tic_id, lc.aperture_id, lc.lightcurve_type_id)
        for lc in db.lightcurves.filter(Lightcurve.id.in_(relevant_ids))
    }
    apertures = [ap.name for ap in db.query(Aperture)]
    lightcurve_types = [lc_t.name for lc_t in db.query(LightcurveType)]

    plan = []
    cur_tmp_id = -1

    for file_obs in cache.query(FileObservation).filter(
        FileObservation.tic_id.in_(cache_subquery.subquery())
    ):
        for ap, lc_t in product(apertures, lightcurve_types):
            lc_key = (file_obs.tic_id, ap, lc_t)
            try:
                id_ = id_map[lc_key]
            except KeyError:
                id_ = cur_tmp_id
                id_map[lc_key] = id_
                cur_tmp_id -= 1

            ingest_job = {
                "lightcurve_id": id_,
                "tic_id": file_obs.tic_id,
                "aperture": ap,
                "lightcurve_type": lc_t,
                "orbit": orbit,
                "camera": file_obs.camera,
                "ccd": file_obs.ccd,
                "file_path": file_obs.file_path,
            }

            plan.append(file_obs)
            seen_cache.add((file_obs.tic_id, file_obs.orbit_number))
    return plan


@lru_cache(maxsize=10)
def get_h5(path):
    return H5File(path, "r")


def load_lightpoints(path, lightcurve_id, aperture, type_):
    constants = get_components(path)
    return parse_h5(get_h5(path), constants, lightcurve_id, aperture, type_)


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
