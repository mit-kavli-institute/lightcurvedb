import itertools
from astropy.io import fits
from collections import defaultdict
from loguru import logger

from lightcurvedb.models.orbit import Orbit
from lightcurvedb.util.tess import sector_from_orbit_number

from multiprocessing import Pool


FITS_TO_ORBIT_MAP = {
    "ORBIT_ID": "orbit_number",
    "SC_RA": "right_ascension",
    "SC_DEC": "declination",
    "SC_ROLL": "roll",
    "SC_QUATX": "quaternion_x",
    "SC_QUATY": "quaternion_y",
    "SC_QUATZ": "quaternion_z",
    "SC_QUATQ": "quaternion_q",
    "CRM": "crm",
    "CRM_N": "crm_n",
}


# Keys which are required to be same across an orbit in order to be
# considered valid.
ORBIT_CONSISTENCY_KEYS = (
    "right_ascension",
    "declination",
    "roll",
    "quaternion_x",
    "quaternion_y",
    "quaternion_z",
    "quaternion_q",
    "crm",
    "crm_n",
)

def _basename_from_fits(path):
    filename = path.stem
    basename, *_ = filename.split("-")
    return basename


def _ffi_to_orbit_kwargs(f):
    with fits.open(f) as fin:
        kwargs = {}
        for key, value in fin[0].header.items():
            try:
                kwargs[FITS_TO_ORBIT_MAP[key]] = value
            except KeyError:
                continue

    kwargs["sector"] = sector_from_orbit_number(kwargs["orbit_number"])
    kwargs["basename"] = _basename_from_fits(f)
    kwargs["file_path"] = f
    return kwargs


def generate_from_fits(files, parallel=True):
    """
    Generate orbits from a list of astropy.fits file paths.

    Parameters
    ----------
    files: pathlib.Path sequence
        An iterable of Paths to consider as fits files.
    parallel: boolean
        If true, extract header information in parallel.

    Returns
    -------
    [Orbit, [str]
        Returns a list of orbit models which have consistent
        astrophysical parameters across their observed frames
        and then the list of FITS files which have been used
        to verify the orbit.
    """
    if parallel:
        with Pool() as p:
            orbit_kwargs = p.map(_ffi_to_orbit_kwargs, files)
    else:
        orbit_kwargs = [
            _ffi_to_orbit_kwargs(file)
            for file in files
        ]

    grouped = defaultdict(list)

    for kwargs in orbit_kwargs:
        grouped[kwargs["orbit_number"]].append(kwargs)

    # Check that all headers are congruent for each found orbit
    new_orbits = []
    for orbit_number, kwargs_array in grouped.items():
        files = [k.pop("file_path") for k in kwargs_array]
        for attr in ORBIT_CONSISTENCY_KEYS:
            ref = kwargs_array[0]
            check = all(
                kwargs[attr] == ref[attr] for kwargs in kwargs_array[1:]
            )
            if not check:
                logger.error(
                    f"FITS files for orbit {orbit_number}"
                        "are not consistent! Rejecting ingestion."
                    )
                return []

        orbit = Orbit(**kwargs)
        logger.debug(f"Generated orbit {orbit}")

        new_orbits.append((
            orbit,
            files,
        ))

    return new_orbits
