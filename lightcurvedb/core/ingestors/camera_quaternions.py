from loguru import logger

from lightcurvedb.models import CameraQuaternion
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
    context = extract_pdo_path_context(str(filepath))
    try:
        camera = context["camera"]
    except (TypeError, KeyError):
        raise ValueError(f"Could not find camera context in {filepath}")
    camera_quaternions = []

    mask = set(
        date
        for date, in db.query(CameraQuaternion.date).filter_by(camera=camera)
    )

    for line in open(filepath, "rt"):
        model = _parse_quat_str(line)
        model.camera = camera

        if model.date in mask:
            logger.warning(
                f"Camera Quaternion uniqueness failed check for {model}, "
                "ignoring"
            )
            continue
        else:
            # Update mask to avoid duplicates inside the file, if any
            mask.add(model.date)

        camera_quaternions.append(model)
    db.session.add_all(camera_quaternions)
    db.flush()


def ingest_directory(db, directory, extension):
    for quat_file in directory.glob(extension):
        ingest_quat_file(db, quat_file)