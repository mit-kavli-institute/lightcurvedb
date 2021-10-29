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
        radius = (
            tic8
            .query(ticentries.c.rad)
            .filter(
                ticentries.c.id == tic_id
            )
            .one()[0]
        )
        if radius is None:
            radius = float("nan")
        return radius * u.solRad


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


class BaseBLSIngestor(Process):
    job_queue = None
    buffers = defaultdict(list)

    def __init__(self, config, name, job_queue):
        self.name = name
        self.config = config
        self.job_queue = job_queue
        self.log("Initialized")
        super().__init__(daemon=True)

    def log(self, msg, level="debug"):
        full_msg = f"{self.name} {msg}"
        getattr(logger, level, logger.debug)(full_msg)

    def load_summary_file(self, tic_id, sector, path):
        # Get inode date change
        date = datetime.fromtimestamp(os.path.getctime(path))
        lines = list(map(lambda l: l.strip(), open(path, "rt").readlines()))

        if len(lines) < 2:
            # No data/malformed bls summary files
            self.log(
                "Unable to parse file {path}",
                level="error"
            )
            raise RuntimeError

        headers = lines[0][2:]
        headers = tuple(map(lambda l: l.lower(), headers.split()))
        lines = lines[1:]
        results = list(normalize(headers, lines))
        stellar_radius = get_stellar_radius(tic_id)
        accepted = []
        with db_from_config(self.config) as db:
            for result in results:
                # Assume that each additional BLS calculate
                result["tce_n"] = int(result.pop("bls_no"))
                result["created_on"] = date
                planet_radius = estimate_planet_radius(
                    stellar_radius, float(result["transit_depth"])
                ).value
                result["transit_duration"] = estimate_transit_duration(
                    result["period"], result["duration_rel_period"]
                )
                result["planet_radius"] = planet_radius
                result["planet_radius_error"] = float("nan")
                result["tic_id"] = int(tic_id)
                result["sector"] = int(sector)


                if "period_inv_transit" not in result:
                    result["period_inv_transit"] = float("nan")
                # Check if bls result already exists
                bls_count = (
                    db
                    .query(
                        BLS
                    )
                    .filter(
                        BLS.tic_id == tic_id,
                        BLS.sector == sector,
                        BLS.tce_n == result["tce_n"]
                    )
                    .count()
                )
                if bls_count > 0:
                    # Ignore
                    self.log(
                        f"Ignoring {path}, currently exists in db",
                        level="warning"
                    )
                    continue
                accepted.append(result)
        return accepted

    def get_best_lightcurve_composition(self, tic_id, sector):
        with db_from_config(self.config) as db:
            composition = (
                db
                .query(
                    BestOrbitLightcurve.id
                )
                .join(
                    BestOrbitLightcurve.orbit
                )
                .join(
                    BestOrbitLightcurve.lightcurve
                )
                .filter(
                    Orbit.sector <= sector,
                    Lightcurve.tic_id == tic_id
                )
                .order_by(
                    Orbit.orbit_number
                )
            )

        return [best_orbit_lc_id for best_orbit_lc_id, in composition]

    def flush(self):
        with db_from_config(self.config) as db:
            while len(self.buffers["bls"]) > 0:
                parameters = self.buffers["bls"].pop()

                tic_id, sector = parameters["tic_id"], parameters["sector"]
                bls = parameters["model"]

                try:
                    db.add(bls)
                    db.flush()
                except ProgrammingError:
                    self.log(f"Could not process {tic_id}", level="warning")
                    db.rollback()
                    continue

                # Build relationships
                many_to_many = []
                for id_ in self.get_best_lightcurve_composition(tic_id, sector):
                    many_to_many.append({
                        "best_detrending_method_id": id_,
                        "bls_id": bls.id,
                    })
                db.session.bulk_insert_mappings(
                    BLSResultLookup,
                    many_to_many
                )
                # Submit new parameters

                db.commit()



    def process_job(self, job):
        path = job["path"]
        tic_id = job["tic_id"]
        sector = job["sector"]
        config_parameters = get_bls_run_parameters(
            job["sector_run_directory"],
            job["camera"]
        )
        all_bls_parameters = self.load_summary_file(tic_id, sector, path)
        for bls_parameters in all_bls_parameters:
            bls_parameters["runtime_parameters"] = config_parameters
            bls_parameters.pop("nan", "")
            bls = BLS(**bls_parameters)
            self.buffers["bls"].append({
                "model": bls,
                "tic_id": tic_id,
                "sector": sector,
            })
        self.log(f"Processed {tic_id}")
        self.flush()
        self.log(f"Submitted {tic_id}", level="success")

    def run(self):
        self.log("Entered main runtime")
        job = self.job_queue.get()
        self.process_job(job)
        while not self.job_queue.empty():
            job = self.job_queue.get()
            self.process_job(job)
