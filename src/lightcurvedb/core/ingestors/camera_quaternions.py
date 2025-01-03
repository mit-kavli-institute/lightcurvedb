import multiprocessing as mp

import sqlalchemy as sa
from loguru import logger

from lightcurvedb.models.camera_quaternion import (
    CameraQuaternion,
    get_utc_time,
)
from lightcurvedb.util.contexts import extract_pdo_path_context

QUAT_FIELD_ORDER = (
    "gps_time",
    "q1",
    "q2",
    "q3",
    "q4",
    "bit_check",
    "total_guide_stars",
    "valid_guide_stars",
)
QUAT_FIELD_TYPES = (
    float,
    float,
    float,
    float,
    float,
    int,
    int,
    int,
)


def get_min_max_datetime(quaternion_path, delimiter=None):
    """
    Obtain min and max datetimes from a camera quaternion file.
    Assumes the file is ordered by ascending datetimes.
    """

    with open(quaternion_path, "rt") as fin:
        line_iter = iter(fin)
        first_line = next(line_iter)
        last_line = first_line
        for last_line in line_iter:
            pass
    first_tokens = first_line.strip().split(
        " " if delimiter is None else delimiter
    )
    last_tokens = last_line.strip().split(
        " " if delimiter is None else delimiter
    )
    first_gps_token = first_tokens[0]
    last_gps_token = last_tokens[0]

    return (
        get_utc_time(first_gps_token).datetime,
        get_utc_time(last_gps_token).datetime,
    )


def _parse_quat_str(string, delimiter=None):
    tokens = string.strip().split(" " if delimiter is None else delimiter)
    tokens = tuple(
        cast(token) for cast, token in zip(QUAT_FIELD_TYPES, tokens)
    )
    kwargs = dict(zip(QUAT_FIELD_ORDER, tokens))

    model = CameraQuaternion()
    # TODO allow assignment of bit and guide star checks
    model.gps_time = kwargs["gps_time"]
    for quat in ("q1", "q2", "q3", "q4"):
        setattr(model, quat, kwargs[quat])
    return model


def ingest_quat_file(db, filepath):
    """
    Attempts to ingest the camera quaternion data stored in the filepath.
    Expects the filepath to contain relevant camera information.

    Parameters
    ----------
    db: lightcurvedb.ORM_DB
        An open database connection
    filepath: str or pathlike
        The location of the camera quaternion file

    Note
    ----
    This function does not call db.commit(), all changes must be committed by
    the callee.
    """
    # Double check we have all the needed contexts from the filepath
    logger.debug(f"Ingesting camera quaternion file: {filepath}")
    context = extract_pdo_path_context(str(filepath))
    try:
        camera = context["camera"]
        logger.debug(f"Ingesting camera {camera} quaternions")
    except (TypeError, KeyError):
        raise ValueError(f"Could not find camera context in {filepath}")
    camera_quaternions = []

    logger.debug("Getting datetime range...")
    min_date, max_date = get_min_max_datetime(filepath)

    logger.debug("Querying for existing camera quaternion timeseries")
    q = sa.select(CameraQuaternion.date).where(
        CameraQuaternion.camera == int(camera),
        CameraQuaternion.date.between(min_date, max_date),
    )
    mask = set(date for date, in db.execute(q))
    logger.debug(f"Comparing file against {len(mask)} quaternions")
    with mp.Pool() as pool:
        models = filter(
            lambda m: m.date not in mask,
            pool.imap_unordered(
                _parse_quat_str,
                open(filepath, "rt").readlines(),
                chunksize=1000,
            ),
        )
        for model in models:
            model.camera = camera
            camera_quaternions.append(model)
    logger.debug(
        f"Pushing {len(camera_quaternions)} new quaternion rows to remote"
    )
    db.bulk_save_objects(camera_quaternions)
    db.flush()


def ingest_directory(db, directory, extension):
    for quat_file in directory.glob(extension):
        ingest_quat_file(db, quat_file)
