"""
This module provides utility functions to TESS specific contexts.
"""


def sector_from_orbit_number(orbit_number: int) -> int:
    return ((orbit_number + 1) // 2) - 4


def orbit_numbers_from_sector(sector: int) -> int:
    return (2 * sector) + 7, (2 * sector) + 8
