"""
Generate data and push to a static file to test for ingestion correctness
"""

import pathlib
from collections import namedtuple
from hypothesis import strategies as st
from . import tess as tess_st

camera_quaternion = namedtuple(
    "camera_quaternion",
    [
        "gps_time",
        "q1", "q2", "q3", "q4",
        "bit_check",
        "total_guide_stars",
        "valid_guide_stars",
    ]
)

@st.composite
def camera_quaternions(draw):
    return draw(
        st.builds(
            camera_quaternion,
            gps_time=tess_st.gps_times(),
            q1=st.floats(allow_nan=False, allow_infinity=False),
            q2=st.floats(allow_nan=False, allow_infinity=False),
            q3=st.floats(allow_nan=False, allow_infinity=False),
            q4=st.floats(allow_nan=False, allow_infinity=False),
            bit_check=st.integers(),
            total_guide_stars=st.integers(),
            valid_guide_stars=st.integers()
        )
    )
