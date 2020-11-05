import os
import numpy as np
from astropy import units as u
from configparser import ConfigParser
from lightcurvedb.util.decorators import suppress_warnings


LEGACY_MAPPER = {
    "bls_npointsaftertransit_1_0": (
        "points_post_transit",
        lambda x: int(float(x)),
    ),
    "bls_npointsintransit_1_0": ("points_in_transit", lambda x: int(float(x))),
    "bls_npointsbeforetransit_1_0": (
        "points_pre_transit",
        lambda x: int(float(x)),
    ),
    "bls_ntransits_1_0": ("transits", lambda x: int(float(x))),
    "bls_qingress_1_0": ("transit_shape", float),
    "bls_qtran_1_0": ("duration_rel_period", float),
    "bls_rednoise_1_0": ("rednoise", float),
    "bls_sde_1_0": ("sde", float),
    "bls_sn_1_0": ("signal_to_noise", float),
    "bls_sr_1_0": ("sr", float),
    "bls_signaltopinknoise_1_0": ("signal_to_pinknoise", float),
    "bls_tc_1_0": ("transit_center", float),
    "bls_whitenoise_1_0": ("whitenoise", float),
    "bls_period_invtransit_1_0": ("period_inv_transit", float),
    "bls_depth_1_0": ("transit_depth", float),
    "bls_period_1_0": ("period", float),
    "bls_no": ("bls_no", int),
}


@suppress_warnings
def estimate_planet_radius(stellar_radius, transit_depth):
    """
    Estimates the planet radius given the stellar radius (in sol radii)
    and transit depth.

    Parameters
    ----------
    stellar_radius : u.solRadii
        The stellar radius in astropy solRadii units.
    transit_depth : float
        The transit depth fit from BLS.

    Returns
    -------
    u.earthRad
        The radius estimation
    """
    radius = np.sqrt(transit_depth) * stellar_radius
    return radius.to(u.earthRad)


@suppress_warnings
def estimate_transit_duration(period, duration_rel_period):
    """
    Estimates the transit duration given the period and the qtran
    field from the BLS result files.

    Parameters
    ----------
    period : float
        The period in days.
    duration_rel_period : float
        The duration of the transit relative to the period

    Returns
    -------
    float
        The transit duration in days
    """
    return period * duration_rel_period


def get_bls_run_parameters(orbit, camera):
    """
    Open each QLP config file and attempt to determine what
    were the parameters for legacy BLS execution.
    """
    run_dir = orbit.get_sector_directory("ffi", "run")
    parser = ConfigParser()

    config_name = "example-lc-pdo{0}.cfg".format(camera)
    path = os.path.join(run_dir, config_name)

    parser.read(path)

    options = parser.options("BLS")

    parameters = {
        "config_parameters": {o: parser.get("BLS", o) for o in options},
        "bls_program": "vartools",
        "legacy": True,
    }

    return parameters


def normalize(headers, lines):
    for line in lines:
        result = {}
        tokens = line.split()
        for token, header in zip(tokens, headers):
            norm, type_ = LEGACY_MAPPER.get(header.lower(), (None, None))
            if not norm:
                continue
            result[norm] = type_(token)
        yield result
