import pathlib
import os

from hypothesis import strategies as st

from lightcurvedb import db_from_config, models
from lightcurvedb.core.base_model import QLPModel

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


@st.composite
def database(draw):
    TEST_PATH = os.path.dirname(os.path.relpath(__file__))
    CONFIG_PATH = os.path.join(TEST_PATH, "..", "config.conf")
    config_path = draw(st.just(CONFIG_PATH))
    db = db_from_config(config_path)

    def close(self):
        if self.depth == 1:
            session = self._session_stack[0]
            for table in reversed(QLPModel.metadata.sorted_tables):
                session.execute(table.delete())
            session.commit()
            session.close()
        super().close()
    db.close = close

    return db


@st.composite
def psql_integers(draw, **overrides):
    return draw(
        st.integers(min_value=-2147483648, max_value=2147483647, **overrides)
    )


@st.composite
def psql_small_integers(draw, **overrides):
    return draw(st.integers(min_value=-32768, max_value=32767, **overrides))


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
            basename=st.text(min_size=1, max_size=256),
        )
    )


@st.composite
def apertures(draw, **overrides):
    return draw(
        st.builds(
            models.Aperture,
            name=overrides.get(
                "name", st.text(min_size=10, max_size=64)
            ).filter(
                lambda name: name not in FORBIDDEN_KEYWORDS and "/" not in name
            ),
            star_radius=st.floats(),
            inner_radius=st.floats(),
            outer_radius=st.floats(),
        )
    )


@st.composite
def frame_types(draw, **overrides):
    return draw(
        st.builds(
            models.frame.FrameType,
            name=overrides.get("name", st.text(min_size=1, max_size=64)),
            description=st.text(),
        )
    )


@st.composite
def lightcurve_types(draw, **overrides):
    return draw(
        st.builds(
            models.LightcurveType,
            name=overrides.get(
                "name", st.text(min_size=10, max_size=64)
            ).filter(
                lambda name: name not in FORBIDDEN_KEYWORDS and "/" not in name
            ),
            description=st.text(),
        )
    )


@st.composite
def frames(draw, **overrides):
    """
    Generate a frame, note that the relations for this frame, namely
    the FrameType and Orbit are not generated using this strategy.
    """
    return draw(
        st.builds(
            models.frame.Frame,
            cadence_type=psql_small_integers(),
            camera=overrides.get("camera", tess_st.cameras()),
            ccd=overrides.get("ccd", st.one_of(tess_st.ccds(), st.none())),
            cadence=overrides.get("cadence", psql_integers()),
            gps_time=st.floats(),
            start_tjd=overrides.get("start_tjd", st.floats()),
            mid_tjd=overrides.get("mid_tjd", st.floats()),
            end_tjd=overrides.get("end_tjd", st.floats()),
            exp_time=overrides.get("exp_time", st.floats()),
            quality_bit=st.booleans(),
            file_path=st.text(),
            orbit=overrides.get("orbit", st.none()),
            orbit_id=overrides.get("orbit_id", st.none()),
            frame_type=overrides.get("frame_type", st.none()),
            frame_type_id=overrides.get("frame_type_od", st.none()),
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
        z=st.floats(),
    )


@st.composite
def spacecraft_ephemeris(draw, **overrides):
    return draw(
        st.builds(
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
    )
