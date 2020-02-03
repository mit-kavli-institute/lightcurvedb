import click
import os
from collections import Counter, defaultdict
from glob import glob
from astropy.io import fits
from .base import ingest


def check_congruence(headers, field):
    counter = Counter()
    for header in headers:
        counter[header[field]] += 1

    return counter

def group_fits(files, field='ORBIT_ID'):
    grouped = defaultdict(list)
    for file in files:
        with fits.open(file) as fits_in:
            header = dict(fits_in[0].header)
            header['FILEPATH'] = file
            grouped[header[field]].append(header)

    return grouped

@ingest.command()
@click.pass_context
@click.argument('poc-orbit-path', type=click.Path(file_okay=False, exists=True))
@click.option('--orbit', '-o', multiple=True, type=int, help='Specified orbits, if nothing is provided orbits will be inferred from FITS files')
@click.option('--allow-multiple', is_flag=True, help='Allow multiple orbits to be ingested')
def ingest_orbit(ctx, poc_orbit_path, orbit, allow_multiple):
    orbits = orbit
    fits_files = glob(os.path.join(poc_orbit_path, '*.fits'))
    if len(fits_files) == 0:
        raise RuntimeError(
            'Number of fits files is 0, please check your specified path.'
        )
    headers = []
    orbital_congruence = group_fits(fits_files)

    if len(orbits) > 0:
        # User wishes to specify orbits
        if not all(orbit in orbital_congruence for orbit in orbits):
            # User specified orbits but some were not found
            return -1
        # Passed checks
    elif allow_multiple:
        # Infer orbits
        click.echo('Inferring {} orbits'.format(len(orbital_congruence)))
    else:
        # Infer orbit
        if len(orbital_congruence) > 1:
            click.echo(click.style('{} orbits found, when only 1 is allowed'.format(len(orbital_congruence)), fg='red'))
            return -1

    for orbit_id, headers in orbital_congruence.items():
        click.echo(click.style('Ingesting orbit {}'.format(orbit_id), bold=True))

    if not ctx['dryrun']:
        # Commit changes to the database
        pass
