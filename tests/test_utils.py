from hypothesis import given
from hypothesis import strategies as st

from lightcurvedb.util import tess


@given(st.integers())
def test_sector_from_orbit_number(orbit_number):
    sector = tess.sector_from_orbit_number(orbit_number)
    assert orbit_number in tess.orbit_numbers_from_sector(sector)


@given(st.integers())
def test_orbit_numbers_from_sector(sector):
    orbit_numbers = tess.orbit_numbers_from_sector(sector)
    for orbit_number in orbit_numbers:
        expected_sector = tess.sector_from_orbit_number(orbit_number)
        assert expected_sector == sector
