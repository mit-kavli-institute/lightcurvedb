import pathlib
import typing

import numpy as np
from loguru import logger

from lightcurvedb.models.orbit import Orbit
from lightcurvedb.util.tess import sector_from_orbit_number

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


def basename_from_fits(path: pathlib.Path) -> str:
    filename = path.stem
    basename, *_ = filename.split("-")
    return basename


def ffi_header_to_orbit_kwargs(header):
    kwargs = {}
    for key, value in header.items():
        try:
            kwargs[FITS_TO_ORBIT_MAP[key]] = value
        except KeyError:
            continue

    return kwargs


def orbit_from_header_group(
    group: list[tuple[dict[str, typing.Any], pathlib.Path]]
) -> Orbit:

    converted_headers = [
        ffi_header_to_orbit_kwargs(header) for header, _ in group
    ]

    check_arrays = {
        field: np.array([header[field] for header in converted_headers])
        for field in ORBIT_CONSISTENCY_KEYS
    }

    for field in ORBIT_CONSISTENCY_KEYS:
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
    orbit = Orbit(**ffi_header_to_orbit_kwargs(group[0][0]))
    orbit.sector = sector_from_orbit_number(orbit.orbit_number)
    orbit.basename = basename_from_fits(group[0][1])
    return orbit
