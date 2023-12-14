import multiprocessing as mp
import os
import pathlib
from itertools import groupby
from typing import Any, Optional

import numpy as np
import sqlalchemy as sa
from astropy.io import fits
from loguru import logger

from lightcurvedb.models import Frame, Orbit
from lightcurvedb.models.frame import FrameType

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


def orbit_from_header_group(
    db, group: list[tuple[dict[str, Any], pathlib.Path]]
) -> tuple[Orbit, bool]:
    consistent_fields = ["ORBIT_ID", "SC_RA", "SC_DEC", "SC_ROLL"]

    check_arrays = {
        field: np.array([header[0][field] for header in group])
        for field in consistent_fields
    }

    for field in consistent_fields:
        check_array = check_arrays[field]
        if np.all(np.isclose(check_array, check_array[0])):
            # All fields are consistent
            continue
        else:
            min_val = check_array.min()
            max_val = check_array.max()
            mean_val = np.nanmedian(check_array)
            std_val = np.nanstd(check_array)

            logger.warning(
                f"Field {field} is inconsistent within orbit. "
                f"Min: {min_val}, Max: {max_val} "
                f"Mean: {mean_val}, stddev: {std_val}"
            )
    orbit = Orbit(
        orbit_number=group[0][0]["ORBIT_ID"],
        right_ascension=group[0][0]["SC_RA"],
        declination=group[0][0]["SC_DEC"],
        roll=group[0][0]["ROLL"],
        quaternion_x=group[0][0]["SC_QUATX"],
        quaternion_y=group[0][0]["SC_QUATY"],
        quaternion_z=group[0][0]["SC_QUATZ"],
        quaternion_q=group[0][0]["SC_QUATQ"],
    )
    return orbit


def _pull_fits_header(
    path: pathlib.Path,
) -> tuple[dict[str, Any], pathlib.Path]:
    """
    Parse the given path as a FITS file and return the primary header
    information as a python dict as well as the initial path parsed.
    """
    with fits.open(path) as fin:
        header = dict(fin[0].header)
    return header, path


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
            frame_type_id=frame_type.id if frame_type else None,
            orbit_id=orbit.id if orbit else None,
        )
    except KeyError as e:
        print(e)
        print("==={0} HEADER===".format(abspath))
        print(repr(header))
        raise


def ingest_directory(
    db,
    frame_type,
    directory: pathlib.Path,
    extension: str,
    n_workers: Optional[int] = None,
):

    files = list(directory.rglob(extension))

    if n_workers:
        with mp.Pool(n_workers) as pool:
            header_pairs = list(pool.imap_unordered(_pull_fits_header, files))
    else:
        header_pairs = list(map(_pull_fits_header, files))

    header_pairs = sorted(header_pairs, key=lambda row: row[0]["ORBIT_ID"])
    orbit_grouped_headers = groupby(
        header_pairs, key=lambda row: row[0]["ORBIT_ID"]
    )

    new_frames = 0
    for orbit_number, group in orbit_grouped_headers:
        orbit_exists_q = sa.select(Orbit).where(
            Orbit.orbit_number == orbit_number
        )
        if db.scalar(orbit_exists_q.count()):
            orbit = db.execute(orbit_exists_q).scalar()
        else:
            orbit = orbit_from_header_group(db, list(group))
            db.add(orbit)
            db.flush()

        existing_files_q = (
            sa.select(Frame.cadence, Frame.camera, Frame.ccd)
            .join(Frame.orbit)
            .join(Frame.frame_type)
            .where(
                Orbit.orbit_number == orbit_number,
                FrameType.name == frame_type,
            )
        )
        keys = set(db.execute(existing_files_q))

        for header, path in group:
            current_key = (
                header["CADENCE"],
                header["CAM"],
                header.get("CCD", None),
            )
            if current_key in keys:
                # File Exists
                logger.warning(f"Frame already exists in database: {path}")
                frame_q = (
                    sa.select(Frame)
                    .join(Frame.frame_type)
                    .join(Frame.orbit)
                    .where(
                        Orbit.orbit_number == orbit_number,
                        FrameType.name == frame_type,
                    )
                )
                frame = db.execute(frame_q).scalar()
            else:
                logger.debug(f"New Frame: {path}")
                frame = from_fits(path, frame_type=frame_type, orbit=orbit)
                db.add(frame)
                new_frames += 1

    logger.debug(f"Found {new_frames} new frames")
    logger.debug("Emitted frames")
