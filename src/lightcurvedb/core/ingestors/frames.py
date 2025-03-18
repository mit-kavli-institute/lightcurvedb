import multiprocessing as mp
import pathlib
import typing
from itertools import groupby

import sqlalchemy as sa
from astropy.io import fits
from loguru import logger
from tqdm import tqdm

from lightcurvedb.core.connection import DB
from lightcurvedb.core.ingestors import orbits as orbit_ingestion
from lightcurvedb.models import Frame, Orbit
from lightcurvedb.models.frame import FRAME_MAPPER_LOOKUP, FrameType

FFI_HEADER = dict[str, typing.Any]


def _pull_fits_header(
    path: pathlib.Path,
) -> tuple[dict[str, typing.Any], pathlib.Path]:
    """
    Parse the given path as a FITS file and return the primary header
    information as a python dict as well as the initial path parsed.
    """
    with fits.open(path, memmap=True) as fin:
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


def from_fits_header(header: FFI_HEADER, frame_type=None, orbit=None) -> Frame:
    """
    Generates a Frame instance from a FITS file.
    Parameters
    ----------
    header : dict
        The pulled header from a FITS file
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
    frame = Frame()
    for attr, value in header.items():
        try:
            model_attr = FRAME_MAPPER_LOOKUP[attr]
        except KeyError:
            continue
        if hasattr(Frame, model_attr):
            if getattr(frame, model_attr) is None:
                setattr(frame, model_attr, value)

    # Stray light is duplicated by camera per FFI, just grab the
    # relevant flag
    stray_light_key = f"STRAYLT{frame.camera}"
    frame.stray_light = try_get(header, stray_light_key)

    if frame_type is not None:
        frame.frame_type_id = frame_type.id
    if orbit is not None:
        frame.orbit_id = orbit.id

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
        logger.debug(
            f"Determining orbit parameters for new orbit {orbit_number}"
        )
        orbit = orbit_ingestion.orbit_from_header_group(header_group)
        db.add(orbit)
        db.flush()
    else:
        logger.debug(f"Mapped orbit {orbit_number} to {orbit}")

    existing_files_q = (
        sa.select(Frame.file_path)
        .join(Frame.orbit)
        .join(Frame.frame_type)
        .where(
            Orbit.sector == orbit.sector,
            FrameType.name == frame_type.name,
        )
    )

    existing_filenames = set(
        (
            pathlib.Path(raw_path).name
            for raw_path in db.scalars(existing_files_q)
        )
    )

    frame_payload: list[Frame] = []
    for header, path in header_group:
        if path.name in existing_filenames:
            # Frame already exists
            continue
        frame = from_fits_header(header, frame_type=frame_type, orbit=orbit)
        frame.file_path = str(path)
        frame.comment = str(frame.comment)
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
        with mp.Pool(n_workers) as pool, tqdm(
            total=len(files), unit=" FITS"
        ) as bar:
            header_pairs = []
            for pair in pool.imap_unordered(_pull_fits_header, files):
                header_pairs.append(pair)
                bar.update()
                bar.refresh()
    else:
        header_pairs = list(map(_pull_fits_header, tqdm(files, unit=" FITS")))

    header_pairs = sorted(header_pairs, key=lambda row: row[0]["ORBIT_ID"])
    orbit_grouped_headers = groupby(
        header_pairs, key=lambda row: row[0]["ORBIT_ID"]
    )

    for orbit_number, group in orbit_grouped_headers:
        fits_files = list(group)
        logger.debug(
            f"Processing orbit {orbit_number} with {len(fits_files)} FITS"
        )
        ingest_orbit(db, frame_type, orbit_number, fits_files)
