import os
import re
import warnings

import numpy as np
import pandas as pd
from lightcurvedb.models.lightcurve import Lightcurve
from sqlalchemy import text
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
    return dict(
        tic_id=int(components["tic"]),
        orbit_number=int(components["orbit"]),
        camera=int(components["camera"]),
        ccd=int(components["ccd"]),
    )


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
        data=dict(
            cadence=cadences,
            barycentric_julian_date=bjd,
            data=values,
            error=errors,
            x_centroid=x_centroids,
            y_centroid=y_centroids,
            quality_flag=quality_flags,
        )
    )
    lightpoints["lightcurve_id"] = lightcurve_id
    for fieldname, constant in constants.items():
        lightpoints[fieldname] = constant
    return lightpoints


@lru_cache(maxsize=16)
def get_h5(path):
    return H5File(path, "r")


def load_lightpoints(cache, path, lightcurve_id, aperture, type_):
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
        return set(list(sorted(missing))[:max_return])
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
