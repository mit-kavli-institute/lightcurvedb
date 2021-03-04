import os
from datetime import datetime, timedelta
from itertools import product
from multiprocessing import Pool

import click
import pandas as pd
from astropy import units as u

from lightcurvedb.cli.base import lcdbcli
from lightcurvedb.cli.types import CommaList
from lightcurvedb.core.ingestors.bls import (
    estimate_planet_radius,
    estimate_transit_duration,
    get_bls_run_parameters,
    normalize,
)
from lightcurvedb.core.tic8 import TIC8_DB
from lightcurvedb.models import BLS, Lightcurve, Orbit
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
        offset = int(result.pop("bls_no"))
        result["created_on"] = date + timedelta(seconds=offset)
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
@click.argument("sectors", type=int, nargs=-1)
@click.option("--cameras", type=CommaList(int), default="1,2,3,4")
@click.option("--ccds", type=CommaList(int), default="1,2,3,4")
@click.option("--n-processes", type=click.IntRange(0), default=32)
def legacy_ingest(ctx, sectors, cameras, ccds, n_processes):
    for sector in sectors:
        tic8 = TIC8_DB().open()
        with ctx.obj["dbconf"] as db:
            orbit = db.orbits.filter(Orbit.sector == sector).first()

            for camera, ccd in product(cameras, ccds):
                bls_dir = orbit.get_sector_directory(
                    "ffi", "cam{0}".format(camera), "ccd{0}".format(ccd), "BLS"
                )
                click.echo(
                    "Processing {0}".format(
                        click.style(bls_dir, bold=True, fg="white")
                    )
                )
                # Load BLS parameters (assuming no change to config)
                parameters = get_bls_run_parameters(orbit, camera)

                files = map(
                    lambda p: os.path.join(bls_dir, p), os.listdir(bls_dir)
                )

                files = list(filter(lambda f: f.endswith(".blsanal"), files))

                tics = list(map(get_tic, files))
                click.echo(
                    "Processing {0} tics, grabbing info...".format(
                        click.style(str(len(tics)), fg="white", bold=True)
                    )
                )
                click.echo("\tObtaining stellar radii")
                q = tic8.mass_stellar_param_q(set(tics), "id", "rad")

                tic_params = pd.read_sql(
                    q.statement, tic8.bind, index_col="id"
                )
                tic8.close()

                click.echo("\tGetting TIC -> ID Map via best aperture table")
                q = db.lightcurves_from_best_aperture(resolve=False)
                q = q.filter(
                    Lightcurve.tic_id.in_(tics),
                    Lightcurve.lightcurve_type_id == "KSPMagnitude",
                )
                id_map = pd.read_sql(
                    q.statement, db.session.bind, index_col="tic_id"
                )

                click.echo("Creating jobs")
                jobs = map(
                    lambda row: (
                        row[1],
                        tic_params.loc[row[0]]["rad"] * u.solRad,
                    ),
                    zip(tics, files),
                )

                click.echo("Multiprocessing BLS results")
                with Pool(n_processes) as pool:
                    results = tqdm(
                        pool.imap(process_summary, jobs), total=len(files)
                    )
                    results = list(results)
                    good_results = filter(lambda args: args[0], results)
                    good_results = list(good_results)
                click.echo(
                    "Parsed {0} BLS summary files. {1} were accepted, "
                    "{2} were rejected.".format(
                        len(results),
                        len(good_results),
                        len(results) - len(good_results),
                    )
                )

                click.echo("Assigning legacy runtime parameters")
                to_insert = []
                missing = []
                for _, bls_bundle in tqdm(good_results):
                    for result in bls_bundle:
                        tic_id = result.pop("tic_id")
                        result["sector"] = sector
                        try:
                            result["lightcurve_id"] = int(
                                id_map.loc[tic_id]["id"]
                            )
                        except TypeError:
                            click.echo("Something went wrong")
                            click.echo(id_map.loc[tic_id]["id"])
                            raise
                        except KeyError:
                            missing.append(tic_id)
                            continue

                        result["runtime_parameters"] = parameters
                        to_insert.append(result)
                if missing:
                    click.echo(missing)
                    click.echo(
                        "Missing {0} tics. Have these lightcurves been "
                        "ingested? Or have the best apertures been set?"
                        " Ingestor was unable to resolve a single id.".format(
                            len(missing)
                        )
                    )

                q = BLS.upsert_q()

                if ctx.obj["dryrun"]:
                    click.echo(
                        "Would attempt to ingest {0} BLS results".format(
                            len(to_insert)
                        )
                    )
                else:
                    click.echo(
                        "Inserting {0} BLS results into database".format(
                            len(to_insert)
                        )
                    )
                    db.session.execute(q, to_insert)
                    db.commit()


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
