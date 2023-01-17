"""
This module provides utility functions to TESS specific contexts.
"""
from datetime import datetime

from astropy.time import Time


def sector_from_orbit_number(orbit_number: int) -> int:
    return ((orbit_number + 1) // 2) - 4


def orbit_numbers_from_sector(sector: int) -> int:
    return (2 * sector) + 7, (2 * sector) + 8


def gps_time_to_datetime(gps_time: float) -> datetime:
    """
    Convert a floating point gps timestamp to a python datetime object.

    Parameters
    ----------
    gps_time: float
        The recorded gps time to convert

    Returns
    -------
    datetime
        The converted time.
    """
    t = Time(gps_time, format="gps")
    return Time(t, format="isot", scale="utc").datetime
