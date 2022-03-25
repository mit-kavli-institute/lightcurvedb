from hypothesis import strategies as st
from lightcurvedb import models
from . import tess as tess_st

@st.composite
def psql_integers(draw, **overrides):
    return draw(
        st.integers(
            min_value=-2147483648,
            max_value=2147483647,
            **overrides
        )
    )

@st.composite
def psql_small_integers(draw, **overrides):
    return draw(
        st.integers(
            min_value=-32768,
            max_value=32767,
            **overrides
        )
    )

@st.composite
def orbits(draw):
    return draw(
        st.builds(
            models.Orbit,
            id=psql_integers(),
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
            crm_n=psql_integers(),
            basename=st.text(min_size=1, max_size=256)
        )
    )
