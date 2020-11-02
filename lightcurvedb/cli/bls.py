import click
import os
import pandas as pd
from math import sqrt
from multiprocessing import Pool
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta
from configparser import ConfigParser
from lightcurvedb.models.bls import BLS, Orbit
from lightcurvedb.cli.base import lcdbcli
from lightcurvedb.cli.types import CommaList
from lightcurvedb.core.tic8 import TIC8_ENGINE, TIC_Entries
from astropy import units as u
from itertools import product, chain


LEGACY_MAPPER = {
    "bls_npointsaftertransit_1_0": "points_post_transit",
    "bls_npointsintransit_1_0": "points_in_transit",
    "bls_npointsbeforetransit_1_0": "points_pre_transit",
    "bls_ntransits_1_0": "transits",
    "bls_qingress_1_0": "transit_shape",
    "bls_qtran_1_0": "duration_rel_period",
    "bls_rednoise_1_0": "rednoise",
    "bls_sde_1_0": "sde",
    "bls_sn_1_0": "signal_to_noise",
    "bls_sr_1_0": "sr",
    "bls_signaltopinknoise_1_0": "signal_to_pinknoise",
    "bls_tc_1_0": "transit_center",
    "bls_whitenoise_1_0": "whitenoise",
    "bls_period_invtransit_1_0": "period_inv_transit",
    "bls_depth_0_1": "transit_depth",
    "bls_period_0_1": "period"
}

TIC8Session = sessionmaker(autoflush=True)
TIC8Session.configure(bind=TIC8_ENGINE)


def process_summary(path, stellar_radius):
    # Get inode date change
    date = datetime.fromtimestamp(os.path.getctime(path))
    lines = map(lambda l: l.strip(), open(path, 'rt').readlines())

    if len(lines) < 2:
        # No data/malformed bls summary files
        return False, []

    headers = lines[0][2:]
    headers = tuple(map(lambda l: l.lower(), headers.split()))
    lines = lines[1:]
    results = [dict(zip(headers, line.split())) for line in lines]

    for offset, result in enumerate(results):
        # Assume that each additional BLS calculate
        result['created_on'] = date + timedelta(seconds=offset)
        planet_radius = sqrt(float(result['transit_depth'])) * stellar_radius
        planet_radius = planet_radius.to(u.earthRad)
        result["planet_radius"] = planet_radius.value

        # TODO estimate error
        result["planet_radius"] = float('nan')

    return True, results


def get_tic(bls_summary):
    return int(
        os.path.basename(bls_summary).split('.')[0]
    )


@lcdbcli.command()
@click.pass_context
@click.argument("sectors", type=int, nargs=-1)
@click.argument("cameras", type=CommaList(int), default="1,2,3,4")
@click.argument("ccds", type=CommaList(int), default="1,2,3,4")
@click.argument("--n-processes", type=click.IntRange(0), default=32)
def legacy_ingest(ctx, sectors, cameras, ccds, n_processes):
    for sector in sectors:
        tic8 = TIC8Session()
        with ctx["dbconf"] as db:
            orbit = db.orbits.filter(Orbit.sector == sector).first()

            for camera, ccd in product(cameras, ccds):

                # Load BLS parameters (assuming no change to config)
                parser = ConfigParser()
                config.read(
                    orbit.get_sector_directory(
                        "ffi", "run",
                        "example-lc-pdo{0}.cfg".format(camera)
                    )
                )

                bls_dir = orbit.get_sector_directory(
                    "ffi",
                    "cam{0}".format(camera),
                    "ccd{0}".format(ccd),
                    "BLS"
                )
                click.echo(
                    "Processing {0}".format(bls_dir)
                )
                files = map(
                    lambda p: os.path.join(bls_dir, p),
                    os.listdir(bls_dir)
                )

                files = list(filter(lambda f: f.endswith('.blsanal'), files))

                tics = list(map(get_tic, files))
                click.echo(
                    "Obtaining stellar radii"
                )
                q = tic8.query(
                    TIC_Entries.c.id.label('tic_id'),
                    TIC_Entries.c.rad,
                ).filter(TIC_Entries.c.id.in_(set(tics)))

                tic_params = pd.read_sql(
                    q.statement,
                    tic8.bind,
                    index_col='tic_id'
                )

                click.echo("Creating jobs")
                jobs = map(
                    lambda tic_id, path: (
                        path,
                        tic_params.loc[tic_id]['rad'] * u.solRad
                    ), zip(tics, files)
                )

                click.echo("Sending jobs to worker pool")
                with Pool(n_processes) as pool:
                    results = pool.imap(process_summary, jobs)
                    good_results = list(
                        filter(lambda status, _: status, results)
                    )
                click.echo(
                    "Parsed {0} BLS summary files. {1} were accepted, "
                    "{2} were rejected.".format(
                        len(results),
                        len(good_results),
                        len(results) - len(good_results)
                    )
                )
                click.echo("Inserting into database")
                db.session.bulk_insert_mappings(
                    BLS,
                    chain.fromiterable(good_results)
                )
                db.commit()
