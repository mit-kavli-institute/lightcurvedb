import click
import os
from itertools import chain
from collections import Counter, defaultdict
from glob import glob
from astropy.io import fits
from lightcurvedb.core.ingestors.orbit_ingestor import OrbitIngestor
from .types import Database

def group_fits(files, field='ORBIT_ID'):
    grouped = defaultdict(list)
    for file in files:
        with fits.open(file) as fits_in:
            header = dict(fits_in[0].header)
            header['FILEPATH'] = file
            header['FILENAME'] = os.path.basename(file)
            grouped[header[field]].append(header)

    return grouped


@click.group()
@click.pass_context
@click.option('--dbconf', default=os.path.expanduser('~/.config/lightcurvedb/db.conf'), type=Database(), help='Specify a database config for connections')
@click.option('--dryrun/--wetrun', default=False, help='If dryrun, no changes will be commited, recommended for first runs')
def lcdbcli(ctx, dbconf, dryrun):
    """Master command for all lightcurve database commandline interaction"""
    if dryrun:
        click.echo(click.style('Running in dryrun mode', fg='green'))
    else:
        click.echo(click.style('Running in wetrun mode!', fg='red'))
    ctx.ensure_object(dict)
    ctx.obj['dryrun'] = dryrun
    ctx.obj['dbconf'] = dbconf


@lcdbcli.command('ingest_orbit')
@click.pass_context
@click.argument('poc-orbit-paths', nargs=-1, type=click.Path(file_okay=False, exists=True))
@click.argument('sector', type=int)
@click.option('--orbit', '-o', multiple=True, type=int, help='Specified orbits, if nothing is provided orbits will be inferred from FITS files')
def ingest_orbit(ctx, poc_orbit_paths, sector, orbit):
    orbits = orbit
    fits_files = list(chain(
        *(glob(os.path.join(path, '*.fits')) for path in poc_orbit_paths)
    ))
    if len(fits_files) == 0:
        raise RuntimeError(
            'Number of fits files is 0, please check your specified path.'
        )
    headers = []
    orbit_groups = group_fits(fits_files)

    with ctx.obj['dbconf'] as db:
        to_add = []
        to_update = []
        for orbit_id, headers in orbit_groups.items():
            ingestor = OrbitIngestor(context_kwargs={'sector': sector})
            for orbit in ingestor.ingest(headers):
                check = db.orbits.filter_by(orbit_number=orbit.orbit_number).one_or_none()
                if check is not None:
                    click.echo(
                        click.style('Encountered existing orbit {}'.format(orbit.orbit_number), fg='yellow')
                    )
                    check.copy_from(orbit)
                    to_update.append(check)
                else:
                    to_add.append(orbit)
                    db.add(orbit)

        if len(to_add) > 0:
            click.echo(click.style('--Will Add--', bold=True, fg='green'))
            for orbit in to_add:
                click.echo(orbit)

        if len(to_update) > 0:
            click.echo(click.style('--Will Update--', bold=True, fg='yellow'))
            for orbit in to_update:
                click.echo(orbit)
            
        if not ctx.obj['dryrun']:
            # Commit changes to the database
            prompt = click.style('Do these changes look ok?', bold=True)
            click.confirm(prompt, abort=True)
            db.commit()
