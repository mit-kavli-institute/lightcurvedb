import os

from astropy.io import fits
from loguru import logger

from lightcurvedb.models.frame import Frame

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
    header = fits.open(abspath)[0].header
    try:
        return Frame(
            cadence_type=header["INT_TIME"],
            camera=header.get("CAM", header.get("CAMNUM", None)),
            ccd=header.get("CCD", header.get("CCDNUM", None)),
            cadence=header["CADENCE"],
            gps_time=header["TIME"],
            start_tjd=header["STARTTJD"],
            mid_tjd=header["MIDTJD"],
            end_tjd=header["ENDTJD"],
            exp_time=header["EXPTIME"],
            quality_bit=header["QUAL_BIT"],
            file_path=abspath,
            frame_type=frame_type,
            orbit=orbit,
        )
    except KeyError as e:
        print(e)
        print("==={0} HEADER===".format(abspath))
        print(repr(header))
        raise


def ingest_directory(db, frame_type, directory, extension, update=False):
    parent_dir = directory.parent
    files = directory.glob(extension)
    new_paths = []
    existing_paths = []

    for path in files:
        if db.query(Frame).filter_by(file_path=str(path)).one() is None:
            # File Exists
            logger.warning(
                f"Frame path already exists in database: {path}"
            )
            existing_paths.append(path)
        else:
            logger.debug(
                f"New Frame path: {path}"
            )
            new_paths.append(path)
    logger.info(
        f"Found {len(new_paths)} new frame paths"
    )
    logger.info(
        f"Found {len(existing_paths)} existing frame paths"
    )

    orbit_map = {}
    for orbit in generate_from_fits(files):
        # Attempt to locate any existing orbit
        q = db.query(Orbit).filter_by(orbit_number=orbit.orbit_number)
        remote_orbit = q.one()
        if remote_orbit is None:
            # Brand new orbit
            db.add(orbit)
            orbit_map[orbit.orbit_number] = orbit
        else:
            orbit_map[remote_orbit.orbit_number] = remote_orbit

    frames = []

    # Flush any pending changes so id sequences and parameters reflect the
    # current transaction state.
    db.flush()

    # Now construct each frame and build relations
    for path in new_paths:
        frame = from_fits(
            path,
            frame_type=frame_type,
            orbit=orbit
        )
        db.add(frame)
        db.flush()

    if update:
        for path in existing_paths:
            raise NotImplementedError
    return frames
