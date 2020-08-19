from __future__ import division, print_function

import click
import os
import pandas as pd
from glob import glob
from itertools import product
from sqlalchemy.orm import sessionmaker
from lightcurvedb import db
from lightcurvedb.models import Orbit 
from lightcurvedb.cli.base import lcdbcli
from lightcurvedb.cli.types import CommaList
from lightcurvedb.util.contexts import extract_pdo_path_context
from lightcurvedb.core.ingestors.cache import IngestionCache
from lightcurvedb.core.ingestors.temp_table import FileObservation
from lightcurvedb.core.tic8 import TIC8_ENGINE

TIC8Session = sessionmaker(autoflush=True)
TIC8Session.configure(bind=TIC8_ENGINE)


@lcdbcli.group()
@click.pass_context
def cache(ctx):
    pass


@cache.command()
@click.pass_context
@click.argument('LC_paths', nargs=-1, type=click.Path(file_okay=False, exists=True))
def update_file_cache(ctx, lc_paths):
    needed_contexts = {'camera', 'ccd', 'orbit_number'}
    cache = IngestionCache()
    for path in lc_paths:
        context = extract_pdo_path_context(path)
        if not all(x in context for x in needed_contexts):
            click.echo(
                    'Could not find needed contexts for path {} found: {}'.format(path, context)
            )
            continue
        h5s = glob(os.path.join(path, '*.h5'))
        existing_files = cache.session.query(
            FileObservation
        ).filter(
            FileObservation.camera == context['camera'],
            FileObservation.ccd == context['ccd'],
            FileObservation.orbit_number == context['orbit_number']
        ).all()
        existing_file_map = {
            (fo.tic_id, fo.camera, fo.ccd, fo.orbit_number): fo.id
            for fo in existing_files
        }

        to_add = []
        for h5 in h5s:
            tic_id = int(
                os.path.basename(h5).split('.')[0]
            )
            key = (
                tic_id,
                int(context['camera']),
                int(context['ccd']),
                int(context['orbit_number'])
            )
            check = existing_file_map.get(key, None)
            if not check:
                check = FileObservation(
                    tic_id=tic_id,
                    file_path=h5,
                    **context
                )
                to_add.append(check)
        cache.session.add_all(to_add)
        click.echo(
            'Added {} new FileObservations'.format(
                len(to_add)
            )
        )
    cache.commit()


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
            df['cameras'] = camera
            df['ccds'] = ccd
            qflag_dfs.append(df)

        qflags = pd.concat(qflag_dfs)
        qflags = qflags.set_index(['cadences', 'cameras', 'ccds'])
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
