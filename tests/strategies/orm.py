import pathlib

from hypothesis import strategies as st

from lightcurvedb import models

from . import tess as tess_st

ORM_STRATEGY_FILE_PATH = pathlib.Path(__file__)
STRATEGY_PATH = ORM_STRATEGY_FILE_PATH.parent
CONFIG_PATH = STRATEGY_PATH.parent / pathlib.Path("config.conf")
FORBIDDEN_KEYWORDS = {
    "\x00",
    "X",
    "Y",
    "Cadence",
    "BJD",
    "QualityFlag",
    "LightCurve",
    "AperturePhotometry",
    "/",
}


def psql_texts(**kwargs):
    alphabet = st.characters(
        blacklist_categories=["C"],
    )

    return st.text(alphabet=alphabet, **kwargs)


def psql_integers(**overrides):
    return st.integers(
        min_value=-2147483648, max_value=2147483647, **overrides
    )


def psql_small_integers(**overrides):
    return st.integers(min_value=-32768, max_value=32767, **overrides)


def orbits():
    return st.builds(
        models.Orbit,
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
        crm_n=psql_small_integers(),
        basename=st.text(min_size=1, max_size=256),
    )


def apertures(
    star_radius=st.floats(),
    inner_radius=st.floats(),
    outer_radius=st.floats(),
    **overrides
):
    return st.builds(
        models.Aperture,
        name=overrides.get(
            "name",
            st.text(min_size=10, max_size=64).filter(
                lambda name: name not in FORBIDDEN_KEYWORDS and "/" not in name
            ),
        ),
        star_radius=star_radius,
        inner_radius=inner_radius,
        outer_radius=outer_radius,
        description=psql_texts(),
    )


def frame_types(**overrides):
    return st.builds(
        models.frame.FrameType,
        name=overrides.get("name", st.text(min_size=1, max_size=64)),
        description=st.text(),
    )


def lightcurve_types(
    name=st.text(min_size=10, max_size=64).filter(
        lambda name: name not in FORBIDDEN_KEYWORDS and "/" not in name
    ),
    **overrides
):
    return st.builds(
        models.LightcurveType,
        name=name,
        description=st.text(),
    )


def frames(**overrides):
    """
    Generate a frame, note that the relations for this frame, namely
    the FrameType and Orbit are not generated using this strategy.
    """
    return st.builds(
        models.frame.Frame,
        cadence_type=psql_small_integers(),
        camera=overrides.get("camera", tess_st.cameras()),
        ccd=overrides.get("ccd", st.one_of(tess_st.ccds(), st.none())),
        cadence=overrides.get("cadence", psql_integers()),
        gps_time=st.floats(),
        start_tjd=overrides.get("start_tjd", tess_st.tjds()),
        mid_tjd=overrides.get("mid_tjd", tess_st.tjds()),
        end_tjd=overrides.get("end_tjd", tess_st.tjds()),
        exposure_time=overrides.get("exposure_time", st.floats()),
        quality_bit=st.booleans(),
        file_path=st.text(),
        orbit=overrides.get("orbit", st.none()),
        orbit_id=overrides.get("orbit_id", st.none()),
        frame_type=overrides.get("frame_type", st.none()),
        frame_type_id=overrides.get("frame_type_od", st.none()),
    )


def camera_quaternions():
    return st.builds(
        models.CameraQuaternion,
        date=st.datetimes(),
        camera=tess_st.cameras(),
        w=st.floats(),
        x=st.floats(),
        y=st.floats(),
        z=st.floats(),
    )


def spacecraft_ephemeris(**overrides):
    return st.builds(
        models.SpacecraftEphemeris,
        barycentric_dynamical_time=overrides.get(
            "barycentric_dynamical_time", tess_st.tjds()
        ),
        calendar_date=st.datetimes(),
        x_coordinate=st.floats(allow_nan=False, allow_infinity=False),
        y_coordinate=st.floats(allow_nan=False, allow_infinity=False),
        z_coordinate=st.floats(allow_nan=False, allow_infinity=False),
        light_travel_time=st.floats(allow_nan=False, allow_infinity=False),
        range_to=st.floats(allow_nan=False, allow_infinity=False),
        range_rate=st.floats(allow_nan=False, allow_infinity=False),
    )
