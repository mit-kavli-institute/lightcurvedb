from hypothesis import given
from hypothesis import strategies as st

from lightcurvedb.util import contexts

from .strategies import tess as tess_st


@given(
    st.text(), st.text(), tess_st.orbits(), tess_st.cameras(), tess_st.ccds()
)
def test_path_context_extraction(prefix, suffix, orbit, cam, ccd):
    template = f"{prefix}/orbit-{orbit}/ffi/cam{cam}/ccd{ccd}/{suffix}"
    context = contexts.extract_pdo_path_context(template)
    assert str(orbit) == context["orbit_number"]
    assert str(cam) == context["camera"]
    assert str(ccd) == context["ccd"]
