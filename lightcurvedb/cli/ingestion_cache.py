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
from lightcurvedb.core.ingestors.temp_table import FileObservation, TIC8Parameters
from lightcurvedb.core.tic8 import TIC8_ENGINE, TIC_Entries

TIC8Session = sessionmaker(autoflush=True)
TIC8Session.configure(bind=TIC8_ENGINE)


def catalog_df(*catalog_files):
    dfs = []
    for csv in catalog_files:
        df = pd.read_csv(
            csv,
            delim_whitespace=True,
            names=['tic_id', 'right_ascension', 'declination', 'tmag', 'x', 'y', 'z', 'q', 'k']
        )
        dfs.append(df)
    dfs = pd.concat(dfs).set_index('tic_id')[['right_ascension', 'declination', 'tmag']]
    dfs = dfs[~dfs.index.duplicated(keep='last')]
    return dfs



@lcdbcli.group()
@click.pass_context
def cache(ctx):
    pass


@cache.command()
@click.pass_context
@click.argument('orbits', type=int, nargs=-1)
@click.option('--force-tic8-query', is_flag=True)
def load_stellar_param(ctx, orbits, force_tic8_query):
    cache = IngestionCache()
    tic8 = TIC8Session() if force_tic8_query else None
    observed_tics = set()
    with ctx.obj['dbconf'] as db:
        for orbit_number in orbits:
            orbit = db.query(Orbit).filter(Orbit.orbit_number == orbit_number).one()
            obs_tics = {r for r, in cache.session.query(FileObservation.tic_id).filter(FileObservation.orbit_number == orbit_number).distinct().all()}
            for tic in obs_tics:
                observed_tics.add(tic)
            if force_tic8_query:
                q = tic8.query(
                    TIC_Entries.c.id.label('tic_id'),
                    TIC_Entries.c.ra,
                    TIC_Entries.c.dec,
                    TIC_Entries.c.tmag
                ).filter(TIC_Entries.c.id.in_(obs_tics))

                tic_params = pd.read_sql(
                    q.statement,
                    tic8.bind,
                    index_col=['tic_id']
                )
            else:
                run_dir = orbit.get_qlp_directory(suffixes=['ffi', 'run'])
                click.echo('Looking for catalogs in {}'.format(run_dir))
                catalogs = glob(os.path.join(run_dir, 'catalog*full.txt'))
                tic_params = catalog_df(*catalogs)

    click.echo('Processing')
    click.echo(tic_params)

    click.echo('Determining what needs to be updated in cache')
    params = []
    tic_params.reset_index(inplace=True)
    for kw in tic_params.to_dict('records'):
        check = cache.session.query(TIC8Parameters).filter(TIC8Parameters.tic_id == kw['tic_id']).one_or_none()
        if check:
            continue
        param = TIC8Parameters(**kw)
        params.append(param)

    click.echo('Updating {} entries'.format(len(params)))
    cache.session.add_all(params)

    if not ctx.obj['dryrun']:
        cache.commit()
    else:
        cache.session.rollback()

    click.echo('Added {} definitions'.format(len(params)))
    click.echo('Done')


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
