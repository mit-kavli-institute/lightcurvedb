from hypothesis import strategies as st
from lightcurvedb import models
from . import tess as tess_st

@st.composite
def psql_str(draw, **overrides):
    return st.text(
        **overrides
    )

@st.composite
def orbits(draw):
    return draw(
        st.builds(
            models.Orbit,
            id=st.integers(),
            orbit_number=tess_st.orbits(),
            sector=tess_st.sectors(),
            right_ascension=st.floats(),
            declination=st.floats(),
            roll=st.floats(),
            quaternion_x=st.floats(),
            quaternion_y=st.floats(),
            quaternion_z=st.floats(),
            quaternion_q=st.floats(),
            crm=st.booleans(),
            crm_n=st.integers(),
            basename=st.text(min_size=1, max_size=256)
        )
    )
