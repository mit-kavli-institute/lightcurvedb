from hypothesis import strategies as st

MIN_TJD, MAX_TJD = 1325.29, 2796.12


def ccds():
    return st.integers(min_value=1, max_value=4)


def cameras():
    return st.integers(min_value=1, max_value=4)


def sectors():
    return st.integers(min_value=1, max_value=1000)


def orbits():
    return st.integers(min_value=9, max_value=2009)


def sector_str(sector=None):
    sector = st.just(sector) if sector else sectors()
    return sector.map(lambda s: f"sector-{s}")


def orbit_str(orbit=None):
    orbit = st.just(orbit) if orbit else orbits()
    return orbit.map(lambda o: f"orbit-{o}")


def camera_str(camera=None):
    camera = st.just(camera) if camera else cameras()
    return camera.map(lambda c: f"cam{c}")


def ccd_str(ccd=None):
    ccd = st.just(ccd) if ccd else ccds()
    return ccd.map(lambda c: f"ccd{c}")


def tic_ids():
    return st.integers(min_value=1, max_value=10005000540)


def cadences():
    return st.integers(min_value=1, max_value=100000)


def quality_flags():
    return st.integers(min_value=0, max_value=1)


def right_ascensions():
    return st.floats(min_value=-180, max_value=180)


def declinations():
    return st.floats(min_value=-180, max_value=180)


def rolls():
    return st.floats(min_value=-180, max_value=180)


def tic_parameters():
    return st.builds(
        dict,
        tic_id=tic_ids(),
        ra=right_ascensions(),
        dec=declinations(),
        tmag=st.floats(allow_nan=False, allow_infinity=False),
        pmra=st.floats(min_value=-180, max_value=180),
        pmdec=st.floats(min_value=-180, max_value=180),
        jmag=st.floats(),
        kmag=st.floats(),
        vmag=st.floats(),
    )


def gps_times():
    """
    Minimum gathered from minimum GPS time stored in the production FRAME
    database.

    There are ~86400 seconds per day, on TESS as of 3/29/2022 selecting from
    the minimum and maximum frames by date and getting the average rate of
    passing time we get the rate of 86400.03756128799 seconds per "day".

    Extrapolating for an additional 5 years we get the upper maximum gps time
    1248116533.95987.


    """
    return st.floats(
        min_value=1216580520.25,
        max_value=1248116533.96,
        allow_nan=False,
        allow_infinity=False,
    )


def tjds():
    return st.floats(
        allow_nan=False,
        allow_infinity=False,
        min_value=MIN_TJD,
        max_value=MAX_TJD,
    )
