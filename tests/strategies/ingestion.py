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


@st.composite
def ffi_headers(draw):
    return draw(
        st.builds(
            dict,
            SIMPLE=st.just(True),
            BITPIX=st.just(32),
            NAXIS=st.just(2),
            NAXIS1=st.integers(min_value=1, max_value=2048),
            NAXIS2=st.integers(min_value=1, max_value=2048),
            EXTEND=st.just(True),
            BSCALE=st.just(1),
            BZERO=st.just(2147483648),
            CRM_N=st.just(10),
            ORBIT_ID=tess_st.orbits(),
            ACS_MODE=st.one_of(st.just("FP"), st.just("CPI")),
            SC_RA=tess_st.right_ascensions(),
            SC_DEC=tess_st.declinations(),
            SC_ROLL=tess_st.rolls(),
            SC_QUATX=st.floats(min_value=0, max_value=1),
            SC_QUATY=st.floats(min_value=0, max_value=1),
            SC_QUATZ=st.floats(min_value=0, max_value=1),
            SC_QUATQ=st.floats(min_value=0, max_value=1),
            TJD_ZERO=st.just(2457000.0),
            STARTTJD=st.floats(allow_nan=False, allow_infinity=False),
            MIDTJD=st.floats(allow_nan=False, allow_infinity=False),
            ENDTJD=st.floats(allow_nan=False, allow_infinity=False),
            EXPTIME=st.floats(allow_nan=False, allow_infinity=False),
            INT_TIME=st.integers(),
            TIME=tess_st.gps_times(),
            PIX_CAT=st.just(0),
            REQUANT=st.just(405),
            DIFF_HUF=st.just(109),
            QUAL_BIT=st.just(0),
            SPM=st.just(2),
            CAM=tess_st.cameras(),
            CADENCE=tess_st.cadences(),
            CRM=st.just(True),
        )
    )
