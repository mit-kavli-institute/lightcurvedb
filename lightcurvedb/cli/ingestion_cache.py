from __future__ import division, print_function

import click
import os
import pandas as pd
from itertools import product
from sqlalchemy.orm import sessionmaker
from lightcurvedb import db
from lightcurvedb.models import Orbit 
from lightcurvedb.cli.base import lcdbcli
from lightcurvedb.cli.types import CommaList
from lightcurvedb.core.ingestors.cache import IngestionCache
from lightcurvedb.core.tic8 import TIC8_ENGINE

TIC8Session = sessionmaker(autoflush=True)
TIC8Session.configure(bind=TIC8_ENGINE)


@lcdbcli.group()
@click.pass_context
def cache(ctx):
    pass


@cache.command()
@click.pass_context
@click.argument('orbits', type=int, nargs=-1)
@click.option('--cameras', type=CommaList(int), default='1,2,3,4')
@click.option('--ccds', type=CommaList(int), default='1,2,3,4')
def quality_flags(ctx, orbits, cameras, ccds):
    with db.open():
        click.echo('Preparing cache and querying for orbits')
        cache = IngestionCache()
        orbits = db.orbits.filter(
            Orbit.orbit_number.in_(orbits)
        ).order_by(Orbit.orbit_number.asc())

        qflag_dfs = []

        for orbit, camera, ccd in product(orbits, cameras, ccds):
            expected_qflag_name = 'cam{}ccd{}_qflag.txt'.format(
                camera,
                ccd
            )
            full_path = os.path.join(
                orbit.get_qlp_run_directory(),
                expected_qflag_name
            )

            click.echo('Parsing {}'.format(full_path))
            df = pd.read_csv(
                full_path,
                delimiter=' ',
                names=['cadences', 'quality_flags'],
                dtype={
                    'cadences': int,
                    'quality_flags': int
                }
            )
            df['camera'] = camera
            df['ccd'] = ccd
            qflag_dfs.append(df)

        qflags = pd.concat(qflag_dfs)
        qflags = qflags.set_index(['cadences', 'camera', 'ccd'])
        qflags = qflags[~qflags.index.duplicated(keep='last')]
        click.echo(
            click.style(
                '==== Will Process ====',
                fg='green',
                bold=True
            )
        )
        click.echo(qflags)

        click.echo('Sending to cache...')
        cache.consolidate_quality_flags(qflags)
        cache.commit()
        click.echo('Done')
