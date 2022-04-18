import pathlib
from hypothesis import strategies as st
from lightcurvedb import models
from lightcurvedb import db_from_config
from . import tess as tess_st

ORM_STRATEGY_FILE_PATH = pathlib.Path(__file__)
STRATEGY_PATH = ORM_STRATEGY_FILE_PATH.parent
CONFIG_PATH = STRATEGY_PATH.parent / pathlib.Path("config.conf")


@st.composite
def database(draw):
    config_path = draw(st.just(CONFIG_PATH))
    return db_from_config(config_path)


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

@st.composite
def frame_types(draw):
    return draw(
        st.builds(
            models.frame.FrameType,
            name=st.text(min_size=1, max_size=64),
            description=st.text()
        )
    )

@st.composite
def frames(draw):
    """
    Generate a frame, note that the relations for this frame, namely
    the FrameType and Orbit are not generated using this strategy.
    """
    return draw(
        st.builds(
            models.frame.Frame,
            cadence_type=psql_small_integers(),
            camera=tess_st.cameras(),
            ccd=st.one_of(tess_st.ccds(), st.none()),
            cadence=psql_integers(),
            gps_time=st.floats(),
            start_tjd=st.floats(),
            mid_tjd=st.floats(),
            end_tjd=st.floats(),
            exp_time=st.floats(),
            quality_bit=st.booleans(),
            file_path=st.text()
        )
    )


@st.composite
def camera_quaternions(draw):
    return st.builds(
        models.CameraQuaternion,
        date=st.datetimes(),
        camera=tess_st.cameras(),
        w=st.floats(),
        x=st.floats(),
        y=st.floats(),
        z=st.floats()
    )


@st.composite
def spacecraft_ephemris(draw):
    return draw(
        st.builds(
            models.SpacecraftEphemris,
            barycentric_dynamical_time=tess_st.tjds(),
            calendar_date=st.datetimes(),
            x_coordinate=st.floats(allow_nan=False, allow_infinity=False),
            y_coordinate=st.floats(allow_nan=False, allow_infinity=False),
            z_coordinate=st.floats(allow_nan=False, allow_infinity=False),
            light_travel_time=st.floats(allow_nan=False, allow_infinity=False),
            range_to=st.floats(allow_nan=False, allow_infinity=False),
            range_rate=st.floats(allow_nan=False, allow_infinity=False)
        )
    )
