import multiprocessing as mp
import os
import pathlib
import typing
from itertools import groupby

import sqlalchemy as sa
from astropy.io import fits
from loguru import logger

from lightcurvedb.core.connection import DB
from lightcurvedb.core.ingestors import orbits as orbit_ingestion
from lightcurvedb.models import Frame, Orbit
from lightcurvedb.models.frame import FrameType

FFI_HEADER = dict[str, typing.Any]

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


def _pull_fits_header(
    path: pathlib.Path,
) -> tuple[dict[str, typing.Any], pathlib.Path]:
    """
    Parse the given path as a FITS file and return the primary header
    information as a python dict as well as the initial path parsed.
    """
    with fits.open(path) as fin:
        header = dict(fin[0].header)
    return header, path


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

        for attr, value in header.items():
            safe_attr = attr.replace("-", "_")
            if hasattr(Frame, safe_attr):
                setattr(frame, safe_attr, value)

        # Stray light is duplicated by camera per FFI, just grab the
        # relevant flag
        stray_light_key = f"STRAYLT{frame.camera}"
        frame.stray_light = try_get(header, stray_light_key)

    if frame_type is not None:
        frame.frame_type_id = frame_type.id
    if orbit is not None:
        frame.orbit_id = orbit.id

    frame.file_path = abspath

    return frame


def ingest_orbit(
    db: DB,
    frame_type: FrameType,
    orbit_number: int,
    header_group: list[tuple[FFI_HEADER, pathlib.Path]],
):
    orbit_q = sa.select(Orbit).where(Orbit.orbit_number == orbit_number)
    orbit = db.scalar(orbit_q)

    if orbit is None:
        orbit = orbit_ingestion.orbit_from_header_group(header_group)
        db.add(orbit)
        db.flush()

    existing_files_q = (
        sa.select(Frame.cadence, Frame.camera, Frame.ccd)
        .join(Frame.orbit)
        .join(Frame.frame_type)
        .where(
            Orbit.orbit_number == orbit_number,
            FrameType.name == frame_type.name,
        )
    )

    keys = set(db.execute(existing_files_q))

    frame_payload: list[Frame] = []
    for header, path in header_group:
        key = (header["CADENCE"], header["CAM"], header.get("CCD", None))
        if key in keys:
            # Frame already exists
            continue
        frame = from_fits(path, frame_type=frame_type, orbit=orbit)
        frame_payload.append(frame)

    logger.debug(f"Pushing frame payload ({len(frame_payload)} frame(s))")
    db.add_all(frame_payload)
    db.flush()
    logger.success(f"Inserted {len(frame_payload)} frame(s)")


def ingest_directory(
    db,
    frame_type,
    directory: pathlib.Path,
    extension: str,
    n_workers: typing.Optional[int] = None,
):
    """
    Recursively ingest a target directory using the given Frame Type. Setting
    n_workers > 1 will utilize multiprocessing to read FITS files in parallel.
    """

    files = list(directory.rglob(extension))
    logger.debug(f"Considering {len(files)} FITS files")

    if n_workers:
        with mp.Pool(n_workers) as pool:
            header_pairs = list(pool.imap_unordered(_pull_fits_header, files))
    else:
        header_pairs = list(map(_pull_fits_header, files))

    header_pairs = sorted(header_pairs, key=lambda row: row[0]["ORBIT_ID"])
    orbit_grouped_headers = groupby(
        header_pairs, key=lambda row: row[0]["ORBIT_ID"]
    )

    for orbit_number, group in orbit_grouped_headers:
        ingest_orbit(db, frame_type, orbit_number, list(group))
