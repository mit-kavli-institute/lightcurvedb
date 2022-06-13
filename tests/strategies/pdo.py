import pathlib
from . import tess as tess_st
from hypothesis import strategies as st


@st.composite
def orbit_path(draw):
    orbit = draw(tess_st.orbits())
    return pathlib.Path(f"orbit-{orbit}")

@st.composite
def sector_path(draw):
    sector = draw(tess_st.sectors())
    return pathlib.Path(f"sector-{sector}")

@st.composite
def camera_path(draw):
    camera = draw(tess_st.cameras())
    return pathlib.Path(f"cam{camera}")
