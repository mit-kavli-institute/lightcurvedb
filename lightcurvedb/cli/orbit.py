import click
import os
from itertools import chain
from astropy.io import fits
from lightcurvedb.core.ingestors.orbit_ingestor import OrbitIngestor
from .utils import find_fits, group_fits
from . import lcdbcli


@lcdbcli.command()
@click.pass_context
@click.argument('poc-orbit-paths', nargs=-1, type=click.Path(file_okay=False, exists=True))
@click.argument('sector', type=int)
@click.option('--orbit', '-o', multiple=True, type=int, help='Specified orbits, if nothing is provided orbits will be inferred from FITS files')
@click.option('--allow-compressed', is_flag=True, help='Allow gzipped fits files to be considered')
def ingest_orbit(ctx, poc_orbit_paths, sector, orbit, allow_compressed):
    orbits = orbit
    fits_files = find_fits(*poc_orbit_paths, allow_compressed=allow_compressed)
    if len(fits_files) == 0:
        raise RuntimeError(
            'Number of fits files is 0, please check your specified path.'
        )
    click.echo('Grouping {} fits files by orbit'.format(len(fits_files)))
    grouped = group_fits(fits_files, field='ORBIT_ID')
    with ctx.obj['dbconf'] as db:
        to_add = []
        to_update = []
        for orbit_id, header_group in grouped:

            headers = list(header_group)

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
