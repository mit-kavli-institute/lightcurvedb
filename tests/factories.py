import numpy as np
from hypothesis import assume
from hypothesis.extra import numpy as np_st
from hypothesis.strategies import floats, text, composite, characters, integers, booleans, one_of, none, from_regex, just, lists, tuples
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

    ap = models.Aperture(
        name=name,
        star_radius=star_radius,
        inner_radius=inner_radius,
        outer_radius=outer_radius
    )

    return ap


@define_strategy
@composite
def orbit(draw, **overrides):
    orb = models.Orbit(
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
    return orb


@define_strategy
@composite
def frame_type(draw, **overrides):
    f_type = models.FrameType(
        name=draw(overrides.pop('name', postgres_text())),
        description=draw(overrides.pop('description', postgres_text()))
    )
    return f_type


@define_strategy
@composite
def lightcurve_type(draw, **overrides):
    lc_type = models.lightcurve.LightcurveType(
        name=draw(overrides.pop('name', from_regex(r'[a-za-Z]{1,64}'))),
        description=draw(overrides.pop('description', postgres_text()))
    )
    return lc_type


@define_strategy
@composite
def frame(draw, **overrides):

    float_kwargs = dict(
        min_value=0,
        exclude_min=True,
        allow_infinity=False,
        allow_nan=False
    )

    tjds = draw(tuples(
        floats(**float_kwargs),
        floats(**float_kwargs),
        floats(**float_kwargs)
    ))
    cadence = draw(overrides.pop('cadence', integers(min_value=0, max_value=PSQL_INT_MAX)))

    sort = sorted(tjds)
    start_tjd = sort[0]
    mid_tjd = sort[1]
    end_tjd = sort[2]

    new_frame = models.Frame(
        cadence_type=draw(overrides.pop('cadence_type', integers(min_value=1, max_value=32767))),
        camera=draw(overrides.pop('camera', integers(min_value=1, max_value=4))),
        cadence=cadence,
        ccd=draw(overrides.pop('ccd', one_of(integers(min_value=1, max_value=4), none()))),

        gps_time=draw(overrides.pop('gps_time', floats(allow_infinity=False, allow_nan=False))),
        start_tjd=start_tjd,
        mid_tjd=mid_tjd,
        end_tjd=end_tjd,
        exp_time=(draw(overrides.pop('exp_time', floats(min_value=0, exclude_min=True, allow_infinity=False, allow_nan=False)))),
        quality_bit=(draw(overrides.pop('quality_bit', booleans()))),
        file_path='{}-{}'.format(cadence, draw(overrides.pop("file_path", postgres_text()))),
        orbit=(draw(overrides.pop('orbit', orbit()))),
        frame_type=(draw(overrides.pop('frame_type', frame_type())))
    )
    return new_frame


@define_strategy
@composite
def orbit_frames(draw):
    target_orbit = draw(orbit())
    f_type = draw(frame_type(name=just('Raw FFI')))

    result = draw(lists(
        frame(
            frame_type=just(f_type),
            orbit=just(target_orbit),
            cadence_type=just(30),
            camera=just(1)
        ),
        min_size=2,
        unique_by=lambda f: f.cadence
    ))
    return result


@define_strategy
@composite
def lightcurve_kwargs(draw, **overrides):
    length = draw(overrides.pop('length', integers(min_value=1, max_value=10)))
    __floating__arr = draw(np_st.arrays(np.float32, (5, length)))

    kwargs = dict()

    kwargs['cadences'] = np.arange(length)
    kwargs['bjd'] = __floating__arr[0]
    kwargs['values'] = __floating__arr[1]
    kwargs['errors'] = __floating__arr[2]
    kwargs['x_centroids'] = __floating__arr[3]
    kwargs['y_centroids'] = __floating__arr[4]
    kwargs['quality_flags'] = draw(np_st.arrays(np.int32, length))

    kwargs['tic_id'] = draw(overrides.pop('tic_id', integers(min_value=1, max_value=PSQL_INT_MAX)))
    kwargs['cadence_type'] = draw(overrides.pop('cadence_type', integers(min_value=1, max_value=32767)))
    kwargs['lightcurve_type'] = draw(overrides.pop('lightcurve_type', lightcurve_type()))
    kwargs['aperture'] = draw(overrides.pop('aperture', aperture()))

    return kwargs


@define_strategy
@composite
def lightcurve(draw, **overrides):
    kwargs = draw(lightcurve_kwargs())
    return models.Lightcurve(
        **kwargs
    )


@define_strategy
@composite
def observation(draw, **overrides):
    tic_id = draw(overrides.pop('tic_id', integers(min_value=1, max_value=PSQL_INT_MAX)))
    camera = draw(overrides.pop('camera', integers(min_value=1, max_value=4)))
    ccd = draw(overrides.pop('ccd', integers(min_value=1, max_value=4)))

    return models.Observation(
        tic_id=tic_id,
        camera=camera,
        ccd=ccd,
        orbit=draw(overrides.pop('orbit', orbit()))
    )


@define_strategy
@composite
def lightcurve_list(draw, tic=None, apertures=None, lightcurve_types=None):
    """
        Strategy for buildling lists of lightcurves.
        If passed apertures and/or lightcurve_types, examples will be drawn
        from the passed parameters. If set to None, the lightcurve_list will
        hold a common aperture/type.
    """

    if apertures:
        aperture_choice = one_of(apertures)
    else:
        aperture_choice = aperture()

    if lightcurve_types:
        type_choice = one_of(lightcurve_types)
    else:
        type_choice = lightcurve_type()
    if tic:
        tic_choice = just(tic)
    else:
        tic_choice = integers(min_value=1, max_value=PSQL_INT_MAX)

    return draw(
        lists(lightcurve(aperture=aperture_choice, lightcurve_type=type_choice, tic_id=tic_choice))
    )
