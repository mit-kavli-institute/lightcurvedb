import os
import click
from astropy.io import fits
from itertools import groupby, product
from glob import glob


def extr_tic(filepath):
    return int(os.path.basename(filepath).split('.')[0])


def find_fits(*paths, **kwargs):
    allow_compressed = kwargs.get('allow_compressed', True)
    exts = ['*.fits.gz', '*.fits'] if allow_compressed else ['*.fits']
    # To avoid duplication of frames strip out the file extensions and
    # keep track of which files we've seen (and skip).

    if allow_compressed:
        click.echo(
            click.style(
                'Allowing compressed FITS files. '
                'Reading these files will be orders '
                'of magnitude slower!',
                fg='yellow'
            )
        )

    # For now, preference to store compressed files
    history = set()
    fits_files = []
    # Order matters, check compressed files first
    for path, ext in product(paths, exts):
        files = glob(os.path.join(path, ext))
        for f in files:
            check = f.split('.')[0]
            if check in history:
                # We've already encountered this file
                continue
            history.add(check)
            fits_files.append(f)

    return fits_files


def find_h5(*paths):
    for path in paths:
        query = glob(os.path.join(path, '*.h5'))
        for result in query:
            yield result


def group_fits(files, field='ORBIT_ID'):
    headers = []
    with click.progressbar(files) as all_files:
        for file in all_files:
            header = dict(fits.getheader(file, 0))
            header['FILEPATH'] = file
            header['FILENAME'] = os.path.basename(file)
            headers.append(header)

    return groupby(headers, lambda h: h[field])


def group_h5(files):
    groups = groupby(sorted(files, key=extr_tic), key=extr_tic)
    for tic, files in groups:
        yield tic, list(files)
