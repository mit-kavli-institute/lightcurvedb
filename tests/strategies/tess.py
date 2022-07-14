from hypothesis import strategies as st


@st.composite
def ccds(draw):
    return draw(st.integers(min_value=1, max_value=4))


@st.composite
def cameras(draw):
    return draw(st.integers(min_value=1, max_value=4))


@st.composite
def sectors(draw):
    return draw(st.integers(min_value=1, max_value=1000))


@st.composite
def orbits(draw):
    return draw(st.integers(min_value=9, max_value=2009))


@st.composite
def sector_str(draw, sector=None):
    sector = draw(st.just(sector)) if sector else draw(sectors())
    return f"sector-{sector}"


@st.composite
def orbit_str(draw, orbit=None):
    orbit = draw(st.just(orbit)) if orbit else draw(orbits())
    return f"orbit-{orbit}"


@st.composite
def camera_str(draw, camera=None):
    camera = draw(st.just(camera)) if camera else draw(cameras())
    return f"cam{camera}"


@st.composite
def ccd_str(draw, ccd=None):
    ccd = draw(st.just(ccd)) if ccd else draw(ccds())
    return f"ccd{ccd}"


@st.composite
def tic_ids(draw):
    return draw(st.integers(min_value=1, max_value=10005000540))


@st.composite
def cadences(draw):
    return draw(st.integers(min_value=1, max_value=100000))


@st.composite
def quality_flags(draw):
    return draw(st.integers(min_value=0, max_value=1))


@st.composite
def tic_parameters(draw):
    return draw(
        st.builds(
            dict,
            tic_id=tic_ids(),
            ra=st.floats(min_value=0, max_value=180),
            dec=st.floats(min_value=0, max_value=180),
            tmag=st.floats(allow_nan=False, allow_infinity=False),
            pmra=st.floats(min_value=-180, max_value=180),
            pmdec=st.floats(min_value=-180, max_value=180),
            jmag=st.floats(),
            kmag=st.floats(),
            vmag=st.floats(),
        )
    )


@st.composite
def gps_times(draw):
    """
    Minimum gathered from minimum GPS time stored in the production FRAME
    database.

    There are ~86400 seconds per day, on TESS as of 3/29/2022 selecting from
    the minimum and maximum frames by date and getting the average rate of
    passing time we get the rate of 86400.03756128799 seconds per "day".

    Extrapolating for an additional 5 years we get the upper maximum gps time
    1248116533.95987.


    """
    return draw(
        st.floats(
            min_value=1216580520.25,
            max_value=1248116533.96,
            allow_nan=False,
            allow_infinity=False,
        )
    )


@st.composite
def tjds(draw, **kwargs):
    return draw(
        st.floats(allow_nan=False, allow_infinity=False, min_value=0.0)
    )


@st.composite
def right_ascensions(draw):
    return draw(st.floats(min_value=-180, max_value=180))


@st.composite
def declinations(draw):
    return draw(st.floats(min_value=-180, max_value=180))


@st.composite
def rolls(draw):
    return draw(st.floats(min_value=-180, max_value=180))