import os
from datetime import datetime, timedelta
from itertools import product
from loguru import logger
from glob import glob
from time import sleep
from multiprocessing import Pool, Manager

import click
import pandas as pd
from astropy import units as u

from lightcurvedb.cli.base import lcdbcli
from lightcurvedb.cli.types import CommaList
from lightcurvedb.core.ingestors.bls import (
    BaseBLSIngestor,
)
from lightcurvedb.core.tic8 import TIC8_DB
from lightcurvedb.models import Lightcurve, Orbit
from lightcurvedb.models.best_lightcurve import BestOrbitLightcurve
from lightcurvedb.models.bls import BLS, BLSResultLookup
from lightcurvedb.util.contexts import extract_pdo_path_context
from tabulate import tabulate
from tqdm import tqdm


def process_summary(args):
    path = args[0]
    stellar_radius = args[1]
    # Get inode date change
    date = datetime.fromtimestamp(os.path.getctime(path))
    lines = list(map(lambda l: l.strip(), open(path, "rt").readlines()))

    tic_id = os.path.basename(path).split(".")[0]

    if len(lines) < 2:
        # No data/malformed bls summary files
        return False, []

    headers = lines[0][2:]
    headers = tuple(map(lambda l: l.lower(), headers.split()))
    lines = lines[1:]
    results = list(normalize(headers, lines))

    for result in results:
        # Assume that each additional BLS calculate
        result["tce"] = int(result.pop("bls_no"))
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

        if "period_inv_transit" not in result:
            result["period_inv_transit"] = float("nan")

    return True, results


def determine_bls_context(db, filepath, context):
    basename = os.path.basename(filepath)
    tic_id = int(basename.split(".")[0])
    sector = int(context["sector"])

    lightcurve_composition = (
        db.query(BestOrbitLightcurve.id)
        .join(BestOrbitLightcurve.orbit)
        .join(BestOrbitLightcurve.lightcurve)
        .join(
            BestApertureMap,
            and_(
                BestApertureMap.tic_id == Lightcurve.tic_id,
                BestApertureMap.aperture_id == Lightcurve.aperture_id,
            ),
        )
        .filter(Orbit.sector <= sector, Lightcurve.tic_id == tic_id)
        .order_by(Orbit.orbit_number)
    )

    return [best_orbit_lc_id for best_orbit_lc_id, in lightcurve_composition]


def get_tic(bls_summary):
    return int(os.path.basename(bls_summary).split(".")[0])


@lcdbcli.group()
@click.pass_context
def bls(ctx):
    """
    BLS Result Commands
    """
    pass


@bls.command()
@click.pass_context
@click.argument(
    "paths", type=click.Path(file_okay=False, exists=True), nargs=-1
)
@click.option("--n-processes", type=click.IntRange(0), default=32)
@click.option(
    "--qlp-data-path",
    type=click.Path(file_okay=False, exists=True),
    default="/pdo/qlp-data",
)
def legacy_ingest(ctx, paths, n_processes, qlp_data_path):
    req_contexts = ("sector", "camera", "ccd")
    path_context_map = {}
    path_files_map = {}

    for path in paths:
        context = extract_pdo_path_context(path)
        if not all(req_context in context for req_context in req_contexts):
            click.echo(
                f"Path {path} does not contain needed contexts (sector, camera, ccd)!"
            )
            click.abort()
        path_context_map[path] = context

    for path in paths:
        pattern = os.path.join(path, "*.blsanal")
        files = glob(pattern)
        path_files_map[path] = files
        click.echo(f"Found {len(files)} bls summary files at {path}")

    workers = []
    manager = Manager()
    job_queue = manager.Queue()

    for path in paths:
        files = path_files_map[path]
        context = path_context_map[path]
        with ctx.obj["dbconf"] as db, TIC8_DB() as tic8:
            sector_run_directory = os.path.join(
                qlp_data_path,
                "sector-{sector}".format(**context),
                "ffi",
                "run",
            )
            logger.debug(
                f"Setting {path} runtime parameters at {sector_run_directory}"
            )
            for summary in tqdm(files, unit=" files"):
                try:
                    job = {
                        "path": summary,
                        "sector": int(context["sector"]),
                        "camera": int(context["camera"]),
                        "sector_run_directory": sector_run_directory,
                        "tic_id": int(os.path.basename(summary).split(".")[0]),
                    }
                except (ValueError, IndexError):
                    logger.warning(f"Unable to parse path {summary}")
                    continue
                job_queue.put(job)

    for i in range(n_processes):
        worker = BaseBLSIngestor(
            ctx.obj["dbconf"]._config, f"Worker-{i}", job_queue
        )
        worker.start()
        workers.append(worker)

    while not job_queue.empty():
        sleep(1)

    for worker in workers:
        worker.join()


@bls.command()
@click.pass_context
@click.argument("tics", type=int, nargs=-1)
@click.option("--parameter", "-p", multiple=True, type=BLS.click_parameters)
def query(ctx, tics, parameter):
    with ctx.obj["dbconf"] as db:
        cols = [getattr(BLS, param) for param in parameter]
        q = (
            db.query(*cols)
            .join(BLS.lightcurve)
            .filter(Lightcurve.tic_id.in_(tics))
        )
        click.echo(tabulate(q.all(), headers=list(parameter)))
