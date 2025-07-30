import sys

import numpy as np
from hypothesis.strategies import (
    booleans,
    builds,
    characters,
    composite,
    floats,
    integers,
    just,
    lists,
    none,
    one_of,
    sampled_from,
    text,
    tuples,
)

from lightcurvedb import models

from .constants import PSQL_INT_MAX, TIC_ID_MAX

ABC = "abcdefghijklmnopqrstuvwxyz"

if sys.version_info.major >= 3:
    alphabet = characters(whitelist_categories=["L", "M", "N", "P", "S", "Z"])
else:
    alphabet = sampled_from(ABC + ABC.upper())


def define_strategy(f):
    return f


@define_strategy
@composite
def postgres_text(draw, **text_args):
    t = draw(
        text(
            alphabet=alphabet,
            min_size=text_args.get("min_size", 1),
            max_size=text_args.pop("max_size", 64),
        )
    )
    return t


celestial_degrees = floats(
    min_value=0, max_value=359, allow_infinity=False, allow_nan=False
)


@define_strategy
@composite
def aperture(draw):

    return draw(
        builds(
            models.Aperture,
            name=just("Aperture_000"),
            star_radius=floats(
                min_value=1, allow_nan=False, allow_infinity=False
            ),
            inner_radius=floats(
                min_value=1, allow_nan=False, allow_infinity=False
            ),
            outer_radius=floats(
                min_value=1, allow_nan=False, allow_infinity=False
            ),
        )
    )


@define_strategy
@composite
def orbit(draw, **overrides):
    orb = models.Orbit(
        id=draw(
            overrides.get("id", integers(min_value=0, max_value=PSQL_INT_MAX))
        ),
        orbit_number=draw(
            overrides.get(
                "orbit_number", integers(min_value=0, max_value=PSQL_INT_MAX)
            )
        ),
        sector=draw(
            overrides.get(
                "orbit_number", integers(min_value=0, max_value=PSQL_INT_MAX)
            )
        ),
        right_ascension=draw(
            overrides.get("right_ascension", celestial_degrees)
        ),
        declination=draw(overrides.get("declination", celestial_degrees)),
        roll=draw(overrides.get("roll", celestial_degrees)),
        quaternion_x=draw(
            overrides.get("quaternion_x", floats(allow_infinity=False))
        ),
        quaternion_y=draw(
            overrides.get("quaternion_y", floats(allow_infinity=False))
        ),
        quaternion_z=draw(
            overrides.get("quaternion_z", floats(allow_infinity=False))
        ),
        quaternion_q=draw(
            overrides.get("quaternion_q", floats(allow_infinity=False))
        ),
        crm_n=draw(
            overrides.get(
                "crm_n", integers(min_value=0, max_value=PSQL_INT_MAX)
            )
        ),
        crm=draw(overrides.get("crm", booleans())),
        basename=draw(overrides.get("basename", postgres_text())),
    )
    return orb


@define_strategy
@composite
def frame_type(draw, **overrides):
    f_type = models.FrameType(
        name=draw(overrides.pop("name", postgres_text())),
        description=draw(overrides.pop("description", postgres_text())),
    )
    return f_type


@define_strategy
@composite
def lightcurve_type(draw, **overrides):

    return draw(
        builds(
            models.lightcurve.LightcurveType,
            name=overrides.pop("name", just("lightcurve_type_0")),
            description=just("lightcurve description"),
        )
    )


@define_strategy
@composite
def frame(draw, **overrides):

    float_kwargs = dict(
        min_value=0, exclude_min=True, allow_infinity=False, allow_nan=False
    )

    tjds = draw(
        tuples(
            floats(**float_kwargs),
            floats(**float_kwargs),
            floats(**float_kwargs),
        )
    )
    cadence = draw(
        overrides.pop("cadence", integers(min_value=0, max_value=PSQL_INT_MAX))
    )
    orbit_id = (draw(overrides.pop("orbit_id", none())),)

    sort = sorted(tjds)
    start_tjd = sort[0]
    mid_tjd = sort[1]
    end_tjd = sort[2]

    new_frame = models.Frame(
        cadence_type=draw(
            overrides.pop(
                "cadence_type", integers(min_value=1, max_value=32767)
            )
        ),
        camera=draw(
            overrides.pop("camera", integers(min_value=1, max_value=4))
        ),
        cadence=cadence,
        ccd=draw(
            overrides.pop(
                "ccd", one_of(integers(min_value=1, max_value=4), none())
            )
        ),
        gps_time=draw(
            overrides.pop(
                "gps_time", floats(allow_infinity=False, allow_nan=False)
            )
        ),
        start_tjd=start_tjd,
        mid_tjd=mid_tjd,
        end_tjd=end_tjd,
        exp_time=(
            draw(
                overrides.pop(
                    "exp_time",
                    floats(
                        min_value=0,
                        exclude_min=True,
                        allow_infinity=False,
                        allow_nan=False,
                    ),
                )
            )
        ),
        quality_bit=(draw(overrides.pop("quality_bit", booleans()))),
        file_path="{}-{}".format(
            cadence, draw(overrides.pop("file_path", postgres_text()))
        ),
        orbit_id=orbit_id,
        frame_type=(draw(overrides.pop("frame_type", frame_type()))),
    )
    return new_frame


@define_strategy
@composite
def orbit_frames(draw):
    target_orbit = draw(orbit())
    f_type = draw(frame_type(name=just("Raw FFI")))

    result = draw(
        lists(
            frame(
                frame_type=just(f_type),
                orbit=just(target_orbit),
                cadence_type=just(30),
                camera=just(1),
                orbit_id=just(target_orbit.id),
            ),
            min_size=1,
            max_size=10,
        )
    )
    for cadence, f in enumerate(result):
        f.cadence = cadence
        f.file_path = "FRAME_{0}".format(f.cadence)

    target_orbit.frames = result
    return target_orbit


@define_strategy
@composite
def lightcurve_kwargs(draw, **overrides):
    kwargs = dict()
    kwargs["tic_id"] = draw(
        overrides.pop("tic_id", integers(min_value=1, max_value=PSQL_INT_MAX))
    )
    kwargs["cadence_type"] = draw(
        overrides.pop("cadence_type", integers(min_value=1, max_value=32767))
    )
    kwargs["lightcurve_type"] = draw(
        overrides.pop("lightcurve_type", lightcurve_type())
    )
    kwargs["aperture"] = draw(overrides.pop("aperture", aperture()))

    return kwargs


@define_strategy
def lightpoint(id_=None):
    return builds(
        models.Lightpoint,
        lightcurve_id=id_ if id_ else integers(min_value=1, max_value=99999),
        cadence=integers(min_value=0, max_value=PSQL_INT_MAX),
        bjd=floats(),
        data=floats(),
        error=floats(),
        x=floats(),
        y=floats(),
        quality_flag=integers(min_value=0, max_value=PSQL_INT_MAX),
    )


@define_strategy
@composite
def lightcurve(draw, **overrides):
    item = draw(
        builds(
            models.Lightcurve,
            tic_id=overrides.get(
                "tic_id", integers(min_value=1, max_value=TIC_ID_MAX)
            ),
            lightcurve_type=lightcurve_type(),
            aperture=aperture(),
        )
    )
    item.lightcurve_type_id = item.lightcurve_type.name
    item.aperture_id = item.aperture.name

    return item


@define_strategy
@composite
def observation(draw, **overrides):
    tic_id = draw(
        overrides.pop("tic_id", integers(min_value=1, max_value=PSQL_INT_MAX))
    )
    camera = draw(overrides.pop("camera", integers(min_value=1, max_value=4)))
    ccd = draw(overrides.pop("ccd", integers(min_value=1, max_value=4)))

    return models.Observation(
        tic_id=tic_id,
        camera=camera,
        ccd=ccd,
        orbit=draw(overrides.pop("orbit", orbit())),
    )


@define_strategy
@composite
def lightcurve_list(
    draw,
    min_size=1,
    max_size=10,
    tic=None,
    apertures=None,
    lightcurve_types=None,
):
    """
    Strategy for building lists of lightcurves.
    If passed apertures and/or lightcurve_types, examples will be drawn
    from the passed parameters. If set to None, the lightcurve_list will
    hold a common aperture/type.
    """

    if apertures:
        aperture_choice = one_of(apertures)
    else:
        aperture_choice = just(draw(aperture()))

    if lightcurve_types:
        type_choice = one_of(lightcurve_types)
    else:
        type_choice = just(draw(lightcurve_type()))

    if tic:
        tic_choice = just(tic)
    else:
        tic_choice = integers(min_value=1, max_value=PSQL_INT_MAX)

    return draw(
        lists(
            lightcurve(
                aperture=aperture_choice,
                lightcurve_type=type_choice,
                tic_id=tic_choice,
            ),
            min_size=min_size,
            max_size=max_size,
        )
    )


quat_params = {
    "min_value": -1.0,
    "max_value": 1.0,
    "allow_nan": False,
    "allow_infinity": False,
}


@define_strategy
def quaternion(missing=False):
    if missing:
        ret = tuples(
            floats(**quat_params), floats(**quat_params), floats(**quat_params)
        )
        return ret

    ret = tuples(
        floats(**quat_params),
        floats(**quat_params),
        floats(**quat_params),
        floats(**quat_params),
    )
    return ret


@define_strategy
def bls(make_lc=True):
    if make_lc:
        lc = lightcurve()
    else:
        lc = none()
    return builds(
        models.BLS(
            lightcurve=lc,
            astronet_score=floats(),
            astronet_version=text(max_size=256),
            runtime_parameters=just({}),
            period=floats(),
            transit_duration=floats(),
            planet_radius=floats(),
            planet_radius_error=floats(),
            points_pre_transit=integers(),
            points_in_transit=integers(),
            points_post_transit=integers(),
            transits=integers(),
            transit_shape=floats(),
            duration_rel_period=floats(),
            rednoise=floats(),
            whitenoise=floats(),
            signal_to_noise=floats(),
            sde=floats(),
            sr=floats(),
            period_inv_transit=floats(),
        )
    )


@define_strategy
@composite
def quality_flag_array(draw, observation=None, target=None, array_length=None):
    """Strategy for generating QualityFlagArray instances.

    Parameters
    ----------
    observation : Observation, optional
        If provided, use this observation. Otherwise, generate one.
    target : Target, optional
        If provided, use this target for target-specific flags.
    array_length : int, optional
        Length of the quality flag array. If None, uses a random length.
    """
    # Define quality flag types
    flag_types = [
        "pixel_quality",
        "cosmic_ray",
        "aperture_quality",
        "data_quality",
    ]

    # Generate array length if not provided
    if array_length is None:
        array_length = draw(integers(min_value=1, max_value=1000))

    # Generate quality flags as 32-bit integers
    # Each bit can represent a different quality condition
    quality_flags = draw(
        lists(
            integers(
                min_value=0, max_value=2**31 - 1
            ),  # Max signed 32-bit int
            min_size=array_length,
            max_size=array_length,
        ).map(lambda x: np.array(x, dtype=np.int32))
    )

    # Build the QualityFlagArray
    return builds(
        models.QualityFlagArray,
        type=sampled_from(flag_types),
        observation=just(observation) if observation else none(),
        target=just(target) if target else none(),
        quality_flags=just(quality_flags),
    )
