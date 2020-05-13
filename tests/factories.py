import numpy as np
from hypothesis import assume
from hypothesis.extra import numpy as np_st
from hypothesis.strategies import floats, text, composite, characters, integers, booleans, one_of, none, from_regex
from lightcurvedb import models

from .constants import CONFIG_PATH, PSQL_INT_MAX


define_strategy = lambda f: f

@define_strategy
@composite
def postgres_text(draw, **text_args):
    t = draw(
        text(
            alphabet=characters(
                blacklist_categories=('C')),
            min_size=text_args.get('min_size', 1),
            max_size=text_args.pop('max_size', 64)
        )
    )
    return t

celestial_degrees = floats(
    min_value=0,
    max_value=359,
    allow_infinity=False,
    allow_nan=False
)


@define_strategy
@composite
def aperture(draw):
    name = draw(from_regex(r'[aA]perture_[a-zA-Z0-9]{1,25}', fullmatch=True))
    star_radius = draw(floats(min_value=1, allow_nan=False, allow_infinity=False))
    inner_radius = draw(floats(min_value=1, allow_nan=False, allow_infinity=False))
    outer_radius = draw(floats(min_value=1, allow_nan=False, allow_infinity=False))

    aperture = models.Aperture(
        name=name,
        star_radius=star_radius,
        inner_radius=inner_radius,
        outer_radius=outer_radius
    )

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
        crm_n=draw(overrides.get('crm_n', integers(min_value=0, max_value=PSQL_INT_MAX))),
        crm=draw(overrides.get('crm', booleans())),
        basename=draw(overrides.get('basename', postgres_text()))
    )
    return orbit

@define_strategy
@composite
def frame_type(draw, **overrides):
    frame_type=models.FrameType(
        name=draw(overrides.pop('name', postgres_text())),
        description=draw(overrides.pop('description', postgres_text()))
    )
    return frame_type

@define_strategy
@composite
def lightcurve_type(draw, **overrides):
    lightcurve_type = models.lightcurve.LightcurveType(
        name=draw(overrides.pop('name', postgres_text())),
        description=draw(overrides.pop('description', postgres_text()))
    )
    return lightcurve_type

@define_strategy
@composite
def frame(draw, **overrides):
    frame = models.Frame(
        cadence_type=draw(overrides.pop('orbit_number', integers(min_value=1, max_value=32767))),
        camera=draw(overrides.pop('camera', integers(min_value=1, max_value=4))),
        cadence=draw(overrides.pop('cadence', integers(min_value=0, max_value=PSQL_INT_MAX))),
        ccd=draw(overrides.pop('ccd', one_of(integers(min_value=1, max_value=4), none()))),

        gps_time=draw(overrides.pop('gps_time', floats(allow_infinity=False, allow_nan=False))),
        start_tjd=(draw(overrides.pop('start_tjd', floats(min_value=0, exclude_min=True, allow_infinity=False, allow_nan=False)))),
        mid_tjd=(draw(overrides.pop('mid_tjd', floats(min_value=0, exclude_min=True, allow_infinity=False, allow_nan=False)))),
        end_tjd=(draw(overrides.pop('end_tjd', floats(min_value=0, exclude_min=True, allow_infinity=False, allow_nan=False)))),
        exp_time=(draw(overrides.pop('exp_time', floats(min_value=0, exclude_min=True, allow_infinity=False, allow_nan=False)))),
        quality_bit=(draw(overrides.pop('quality_bit', booleans()))),
        file_path=(draw(overrides.pop('file_path', postgres_text()))),
        orbit=(draw(overrides.pop('orbit', orbit()))),
        frame_type=(draw(overrides.pop('frame_type', frame_type())))
    )
    return frame


@define_strategy
@composite
def lightcurve(draw, **overrides):
    length = draw(overrides.pop('length', integers(min_value=1, max_value=10)))

    __floating__arr = draw(np_st.arrays(np.float32, (5, length)))

    cadences = draw(np_st.arrays(np.int32, length, unique=True))
    bjd = __floating__arr[0]
    values = __floating__arr[1]
    errors = __floating__arr[2]
    x_centroids = __floating__arr[3]
    y_centroids = __floating__arr[4]
    quality_flags = draw(np_st.arrays(np.int32, length))

    tic_id = draw(overrides.pop('tic_id', integers(min_value=1, max_value=PSQL_INT_MAX)))
    cadence_type = draw(overrides.pop('cadence_type', integers(min_value=1, max_value=32767)))
    lc_type = draw(overrides.pop('lightcurve_type', lightcurve_type()))
    ap = draw(overrides.pop('aperture', aperture()))

    return models.Lightcurve(
        id=draw(overrides.pop('with_id', none())),
        cadences=cadences,
        bjd=bjd,
        values=values,
        errors=errors,
        x_centroids=x_centroids,
        y_centroids=y_centroids,
        quality_flags=quality_flags,
        tic_id=tic_id,
        cadence_type=cadence_type,
        lightcurve_type=lc_type,
        aperture=ap
    )

