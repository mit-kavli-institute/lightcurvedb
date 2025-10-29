"""
This module provides utility functions to TESS specific contexts.
"""
from datetime import datetime

from astropy.time import Time


def sector_from_orbit_number(orbit_number: int) -> int:
    sector: int | None = None
    while sector is None:
        sector_spec = input(f"Enter Sector # for Orbit {orbit_number}: ")
        try:
            sector = int(sector_spec)
        except ValueError:
            print(
                f"Could not interpret '{sector_spec}' "
                "as an integer, try again..."
            )

    return sector


def orbit_numbers_from_sector(sector: int) -> tuple[int, ...]:
    import sqlalchemy as sa

    from lightcurvedb import db
    from lightcurvedb import models as m

    with db:
        q = (
            sa.select(m.Orbit.orbit_number)
            .order_by(m.Orbit.orbit_number.asc())
            .where(m.Orbit.sector == sector)
        )
        orbits = list(db.scalars(q))
    return tuple(orbits)


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
