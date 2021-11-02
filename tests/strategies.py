import sys
from hypothesis import strategies as st
from lightcurvedb import models
from .constants import PSQL_INT_MAX, TIC_ID_MAX

PSQL_INT = st.integers(min_value=-1 * PSQL_INT_MAX, max_value=PSQL_INT_MAX)

DEFINED_FLOAT = st.floats(allow_nan=False, allow_infinity=False)

FINITE_FLOAT = st.floats(allow_infinity=False)
CELESTIAL_FLOAT = st.floats(
    min_value=0,
    max_value=360,
    allow_infinity=False,
    allow_nan=False,
)
FINITE_NONZERO_FLOAT = st.floats(
    min_value=0,
    exclude_min=True,
    allow_infinity=False,
    allow_nan=False,
)

if sys.version_info.major >= 3:
    alphabet = st.characters(
        whitelist_categories=["L", "M", "N", "P", "S", "Z"]
    )
else:
    ABC = "abcdefghijklmnopqrstuvwxyz"
    alphabet = st.sampled_from(ABC + ABC.upper())


def postgres_text(**text_args):
    return st.text(alphabet=alphabet, **text_args)


def apertures(**overrides):
    return st.builds(
        models.Aperture,
        name=overrides.get("name", st.just("Aperture_000")),
        star_radius=overrides.get("star_radius", DEFINED_FLOAT),
        inner_radius=overrides.get("inner_radius", DEFINED_FLOAT),
        outer_radius=overrides.get("outer_radius", DEFINED_FLOAT),
    )


def orbits(**overrides):
    return st.builds(
        models.Orbit,
        id=overrides.get("id", PSQL_INT),
        orbit_number=overrides.get("orbit_number", PSQL_INT),
        sector=overrides.get("sector", PSQL_INT),
        right_ascension=overrides.get("right_ascension", CELESTIAL_FLOAT),
        declination=overrides.get("declination", CELESTIAL_FLOAT),
        roll=overrides.get("roll", CELESTIAL_FLOAT),
        quaternion_x=overrides.get("quaternion_x", FINITE_FLOAT),
        quaternion_y=overrides.get("quaternion_y", FINITE_FLOAT),
        quaternion_z=overrides.get("quaternion_z", FINITE_FLOAT),
        quaternion_q=overrides.get("quaternion_q", FINITE_FLOAT),
        crm_n=overrides.get(
            "crm_n", st.integers(min_value=0, max_value=PSQL_INT_MAX)
        ),
        crm=overrides.get("crm", st.booleans()),
        basename=overrides.get(
            "basename", postgres_text(min_size=1, max_size=64)
        ),
    )


def frame_types(**overrides):
    return st.builds(
        models.FrameType,
        name=overrides.get("name", postgres_text(min_size=1, max_size=64)),
        description=overrides.get("description", postgres_text()),
    )


def frames(**overrides):
    return st.builds(
        models.Frame,
        cadence_type=overrides.get(
            "cadence_type", st.integers(min_value=1, max_value=32767)
        ),
        camera=overrides.get("camera", st.integers(min_value=1, max_value=4)),
        ccd=overrides.get("ccd", st.integers(min_value=1, max_value=4)),
        cadence=overrides.get(
            "cadence", st.integers(min_value=0, max_value=PSQL_INT_MAX)
        ),
        gps_time=overrides.get("gps_time", DEFINED_FLOAT),
        start_tjd=overrides.get("start_tjd", FINITE_NONZERO_FLOAT),
        mid_tjd=overrides.get("mid_tjd", FINITE_NONZERO_FLOAT),
        end_tjd=overrides.get("end_tjd", FINITE_NONZERO_FLOAT),
        exp_time=overrides.get("exp_time", FINITE_NONZERO_FLOAT),
        quality_bit=overrides.get("quality_bit", st.booleans()),
        file_path=overrides.get("file_path", postgres_text()),
        orbit=overrides.get("orbit", orbits()),
        frame_type=overrides.get("frame_type", frame_types()),
    )


# Define composite strategies here.
# Composite meaning strategies that follow QLPModel relationships and
# must have their data be "valid" in view of Postgres.


def orbit_frames(data, orbit, frame_type):
    frame_list = data.draw(
        st.lists(
            frames(orbit=st.just(orbit), frame_type=st.just(frame_type)),
            min_size=1,
            max_size=10,
        )
    )
    for ith, frame in enumerate(frame_list):
        frame.cadence = ith
        frame.file_path = "frame_{0}".format(ith)

    return frame_list
