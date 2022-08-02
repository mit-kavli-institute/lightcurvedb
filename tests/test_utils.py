from hypothesis import given
from hypothesis import strategies as st

from .strategies import tess as tess_st

from lightcurvedb.util import tess, contexts


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


@given(st.text(), st.text(), tess_st.orbits(), tess_st.cameras(), tess_st.ccds())
def test_path_context_extraction(prefix, suffix, orbit, cam, ccd):
    template = f"{prefix}/orbit-{orbit}/ffi/cam{cam}/ccd{ccd}/{suffix}"
    context = contexts.extract_pdo_path_context(template)
    assert str(orbit) == context["orbit_number"]
    assert str(cam) == context["camera"]
    assert str(ccd) == context["ccd"]
