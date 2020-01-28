from hypothesis import assume
from hypothesis.strategies import floats, text, composite, characters, integers, booleans
from hypothesis_fspaths import fspaths
from lightcurvedb.core.base_model import QLPModel
from lightcurvedb.core.connection import db_from_config
from lightcurvedb import models

from .constants import CONFIG_PATH, PSQL_INT_MAX


define_strategy = lambda f: f

postgres_text = text(alphabet=characters(
    blacklist_categories=('Cc', 'Cs'),
    blacklist_characters=[u'\x00', u'\u0000']),
    min_size=1,
    max_size=64)

celestial_degrees = floats(
    min_value=0,
    max_value=359,
    allow_infinity=False,
    allow_nan=False
)


@define_strategy
@composite
def aperture(draw, save=False):
    name=draw(postgres_text)
    star_radius = draw(floats(min_value=1, allow_nan=False, allow_infinity=False))
    inner_radius = draw(floats(min_value=1, allow_nan=False, allow_infinity=False))
    outer_radius = draw(floats(min_value=1, allow_nan=False, allow_infinity=False))

    assume(inner_radius < outer_radius)
    aperture = models.Aperture(
        name=name,
        star_radius=star_radius,
        inner_radius=inner_radius,
        outer_radius=outer_radius
    )

    if save:
        with db_from_config(CONFIG_PATH).open() as db:
            db.add(aperture)
            db.commit()
            db.session.refresh(aperture)

    return aperture

@define_strategy
@composite
def orbit(draw, **overrides):
    orbit = models.Orbit(
        orbit_number=draw(overrides.get('orbit_number', integers(min_value=0, max_value=PSQL_INT_MAX))),
        sector=draw(overrides.get('orbit_number', integers(min_value=0, max_value=PSQL_INT_MAX))),
        right_ascension=draw(overrides.get('right_ascension', celestial_degrees)),
        declination=draw(overrides.get('declination', celestial_degrees)),
        roll=draw(overrides.get('roll', celestial_degrees)),
        quaternion_x=draw(overrides.get('quaternion_x', floats(allow_infinity=False))),
        quaternion_y=draw(overrides.get('quaternion_y', floats(allow_infinity=False))),
        quaternion_z=draw(overrides.get('quaternion_z', floats(allow_infinity=False))),
        quaternion_q=draw(overrides.get('quaternion_q', floats(allow_infinity=False))),
        crm_n=draw(overrides.get('crm_n', integers(min_value=0, max_value=1))),
        basename=draw(overrides.get('basename', postgres_text))
    )
    return orbit

@define_strategy
@composite
def frame_type(draw, **overrides):
    frame_type=models.FrameType(
        name=draw(overrides.pop('name', postgres_text)),
        description=draw(overrides.pop('description', postgres_text))
    )
    return frame_type

@define_strategy
@composite
def frame(draw, **overrides):
    frame = models.Frame(
        cadence_type=draw(overrides.pop('orbit_number', integers(min_value=1, max_value=32767))),
        camera=draw(overrides.pop('camera', integers(min_value=1, max_value=4))),
        cadence=draw(overrides.pop('cadence', integers(min_value=0, max_value=PSQL_INT_MAX))),

        gps_time=draw(overrides.pop('gps_time', floats(allow_infinity=False, allow_nan=False))),
        start_tjd=(draw(overrides.pop('start_tjd', floats(min_value=0, exclude_min=True, allow_infinity=False, allow_nan=False)))),
        mid_tjd=(draw(overrides.pop('mid_tjd', floats(min_value=0, exclude_min=True, allow_infinity=False, allow_nan=False)))),
        end_tjd=(draw(overrides.pop('end_tjd', floats(min_value=0, exclude_min=True, allow_infinity=False, allow_nan=False)))),
        exp_time=(draw(overrides.pop('exp_time', floats(min_value=0, exclude_min=True, allow_infinity=False, allow_nan=False)))),
        quality_bit=(draw(overrides.pop('quality_bit', booleans()))),
        file_path=(draw(overrides.pop('file_path', fspaths()))),
        orbit=(draw(overrides.pop('orbit', orbit()))),
        frame_type=(draw(overrides.pop('frame_type', frame_type())))
    )
    return frame