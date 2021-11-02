import os
import numpy as np
from datetime import datetime
from collections import defaultdict
from astropy import units as u
from functools import lru_cache
from configparser import ConfigParser
from loguru import logger
from sqlalchemy import and_
from sqlalchemy.exc import ProgrammingError
from lightcurvedb import db_from_config
from lightcurvedb.models.aperture import BestApertureMap
from lightcurvedb.models.bls import BLSResultLookup, BLS
from lightcurvedb.models.best_lightcurve import BestOrbitLightcurve
from lightcurvedb.models.lightcurve import Lightcurve
from lightcurvedb.models.orbit import Orbit
from lightcurvedb.util.decorators import suppress_warnings
from lightcurvedb.core.tic8 import TIC8_DB
from lightcurvedb.core.ingestors.consumer import BufferedDatabaseIngestor
from multiprocessing import Process


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


def get_stellar_radius(tic_id):
    with TIC8_DB() as tic8:
        ticentries = tic8.ticentries
        radius, radius_error = (
            tic8.query(ticentries.c.rad, ticentries.c.e_rad)
            .filter(ticentries.c.id == tic_id)
            .one()
        )
        if radius is None:
            radius = float("nan")
        if radius_error is None:
            radius_error = float("nan")

        return radius * u.solRad, radius_error * u.solRad


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


@lru_cache
def get_bls_run_parameters(sector_run_directory, camera):
    """
    Open each QLP config file and attempt to determine what
    were the parameters for legacy BLS execution.
    """
    parser = ConfigParser()

    config_name = "example-lc-pdo{0}.cfg".format(camera)
    path = os.path.join(sector_run_directory, config_name)

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
            if norm is None:
                continue
            result[norm] = type_(token)
        yield result


class BaseBLSIngestor(BufferedDatabaseIngestor):
    job_queue = None
    buffers = defaultdict(list)
    seen_cache = set()
    buffer_order = ["bls"]

    def __init__(self, config, name, job_queue):
        super().__init__(config, name, job_queue)

    def load_summary_file(self, tic_id, sector, path):
        # Get inode date change
        date = datetime.fromtimestamp(os.path.getctime(path))
        lines = list(map(lambda l: l.strip(), open(path, "rt").readlines()))

        if len(lines) < 2:
            # No data/malformed bls summary files
            self.log("Unable to parse file {path}", level="error")
            raise RuntimeError

        headers = lines[0][2:]
        headers = tuple(map(lambda l: l.lower(), headers.split()))
        lines = lines[1:]
        results = list(normalize(headers, lines))
        stellar_radius, stellar_radius_error = get_stellar_radius(tic_id)
        accepted = []
        for result in results:
            # Assume that each additional BLS calculate
            result["tce_n"] = int(result.pop("bls_no"))
            result["created_on"] = date
            planet_radius = estimate_planet_radius(
                stellar_radius, float(result["transit_depth"])
            ).value
            planet_radius_error = estimate_planet_radius(
                stellar_radius_error, float(result["transit_depth"])
            ).value
            result["transit_duration"] = estimate_transit_duration(
                result["period"], result["duration_rel_period"]
            )
            result["planet_radius"] = (
                planet_radius if not np.isnan(planet_radius) else float("nan")
            )
            result["planet_radius_error"] = (
                planet_radius_error
                if not np.isnan(planet_radius_error)
                else float("nan")
            )
            result["tic_id"] = int(tic_id)
            result["sector"] = int(sector)

            if "period_inv_transit" not in result:
                result["period_inv_transit"] = float("nan")
            accepted.append(result)
        return accepted

    def flush_bls(self, db):
        self.log("Flushing bls entries")
        tic_ids = {param["tic_id"] for param in self.buffers["bls"]}
        keys = ("sector", "tic_id", "tce_n")
        cache = set(
            db.query(*[getattr(BLS, key) for key in keys]).filter(
                BLS.tic_id.in_(tic_ids)
            )
        )
        self.log(
            f"Filtering {len(self.buffers['bls'])} bls results against {len(cache)} relevant entries in db"
        )

        db.session.bulk_insert_mappings(
            BLS,
            filter(
                lambda param: tuple(param[key] for key in keys) not in cache,
                self.buffers["bls"],
            ),
        )
        self.log("Submitted bls entries")

    def process_job(self, job):
        path = job["path"]
        tic_id = job["tic_id"]
        sector = job["sector"]
        config_parameters = get_bls_run_parameters(
            job["sector_run_directory"], job["camera"]
        )
        all_bls_parameters = self.load_summary_file(tic_id, sector, path)
        for bls_parameters in all_bls_parameters:
            bls_parameters["runtime_parameters"] = config_parameters
            self.buffers["bls"].append(bls_parameters)

        self.log(f"Processed {path}")

    @property
    def should_flush(self):
        return len(self.buffers["bls"]) >= 10000
