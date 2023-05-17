import os
import typing

from astropy.io import fits
from loguru import logger

from lightcurvedb.models import Frame, Orbit

from .orbits import generate_from_fits

FITS_TO_FRAME_MAP = {
    "cadence_type": "INT_TIME",
    "camera": ["CAM", "CAMNUM"],
    "ccd": ["CCD", "CCDNUM"],
    "cadence": "CADENCE",
    "gps_time": "TIME",
    "start_tjd": "STARTTJD",
    "mid_tjd": "MIDTJD",
    "end_tjd": "ENDTJD",
    "exp_time": "EXPTIME",
    "quality_bit": "QUAL_BIT",
}


def _resolve_fits_value(header, key):
    if isinstance(key, str):
        return header[key]

    # Assume key is iterable of primary and fallback keys
    try:
        return header[key[0]]
    except KeyError:
        # Try fallback
        if len(key) < 2:
            raise ValueError(
                f"Could not resolve {key} in header. Out of fallbacks."
            )
        return _resolve_fits_value(header, key[1:])


def _ffi_to_frame_kwargs(path):
    kwargs = {}
    with fits.open(path) as fin:
        header = fin[0].header
        for frame_key, header_keys in FITS_TO_FRAME_MAP:
            value = _resolve_fits_value(header, header_keys)
            kwargs[frame_key] = value
    return kwargs


def try_get(header, key, *fallbacks, default=None) -> typing.Any:
    try:
        return header[key]
    except KeyError:
        for k in fallbacks:
            try:
                return header[k]
            except KeyError:
                continue
    return default


def from_fits(path, frame_type=None, orbit=None):
    """
    Generates a Frame instance from a FITS file.
    Parameters
    ----------
    path : str or pathlike
        The path to the FITS file.
    frame_type : FrameType, optional
        The FrameType relation for this Frame instance, by default this
        is not set (None).
    orbit : Orbit
        The orbit this Frame was observed in. By default this is not set
        (None).

    Returns
    -------
    Frame
        The constructed frame.
    """
    abspath = os.path.abspath(path)

    frame = Frame()
    with fits.open(abspath) as fin:
        header = fin[0].header
        try:
            frame.cadence_type = header["INT_TIME"]
            frame.camera = try_get(header, "CAM", "CAMNUM")
            frame.ccd = try_get(header, "CCD", "CCDNUM")
            frame.cadence = header["CADENCE"]
            frame.gps_time = header["TIME"]
            frame.start_tjd = header["STARTTJD"]
            frame.mid_tjd = header["MIDTJD"]
            frame.end_tjd = header["ENDTJD"]
            frame.exp_time = header["EXPTIME"]
            frame.quality_bit = header["QUAL_BIT"]
            frame.fine_pointing = try_get(header, "FINE")
            frame.coarse_pointing = try_get(header, "COARSE")
            frame.reaction_wheel_desaturation = try_get(header, "RW_DESAT")

            stray_light_key = f"STRAYLT{frame.camera}"
            frame.stray_light = try_get(header, stray_light_key)
        except KeyError as e:
            print(e)
            print("==={0} HEADER===".format(abspath))
            print(repr(header))
            raise

    if frame_type is not None:
        frame.frame_type_id = frame_type.id
    if orbit is not None:
        frame.orbit_id = orbit.id

    return frame


def ingest_directory(db, frame_type, directory, extension, update=False):
    files = list(directory.glob(extension))
    new_paths = []
    existing_paths = []

    for path in files:
        q = db.query(Frame).filter_by(file_path=str(path))
        if q.one_or_none() is not None:
            # File Exists
            logger.warning(f"Frame path already exists in database: {path}")
            existing_paths.append(path)
        else:
            logger.debug(f"New Frame path: {path}")
            new_paths.append(path)

    logger.debug(f"Found {len(new_paths)} new frame paths")
    logger.debug(f"Found {len(existing_paths)} existing frame paths")

    orbit_map = {}
    for orbit, paths in generate_from_fits(files, parallel=True):
        # Attempt to locate any existing orbit
        q = db.query(Orbit).filter_by(orbit_number=orbit.orbit_number)
        remote_orbit = q.one_or_none()
        if remote_orbit is None:
            # Brand new orbit
            db.add(orbit)
        else:
            orbit = remote_orbit

        for path in paths:
            orbit_map[path] = orbit

    db.flush()

    frames = []

    # Now construct each frame and build relations
    for path in new_paths:
        logger.debug(path)
        frame = from_fits(path, frame_type=frame_type, orbit=orbit_map[path])
        frames.append(frame)

    logger.debug("Pushing to remote")
    db.bulk_save_objects(frames)
    db.flush()
    logger.debug("Emitted frames")

    if update:
        for path in existing_paths:
            raise NotImplementedError
    return frames
