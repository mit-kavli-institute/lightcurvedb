import os
import click
from astropy.io import fits
from collections import defaultdict
from itertools import groupby, chain, product
from glob import glob


def find_fits(*paths, allow_compressed=True):
    exts = ['*.fits.gz', '*.fits'] if allow_compressed else ['*.fits']
    # To avoid duplication of frames strip out the file extensions and
    # keep track of which files we've seen (and skip).

    if allow_compressed:
        click.echo(
            click.style('Allowing compressed FITS files. Reading these files will be orders of magnitude slower!', fg='yellow')
        )

    # For now, preference to store compressed files
    history = set()
    fits_files = []
    # Order matters, check compressed files first
    for path, ext in product(paths, exts):
        files = glob(os.path.join(path, ext))
        for f in files:
            basefilename = os.path.basename(f)
            check = f.split('.')[0]
            if check in history:
                # We've already encountered this file
                continue
            history.add(check)
            fits_files.append(f)

    return fits_files


def group_fits(files, field='ORBIT_ID'):
    headers = []
    with click.progressbar(files) as all_files:
        for file in all_files:
            header = dict(fits.getheader(file, 0))
            header['FILEPATH'] = file
            header['FILENAME'] = os.path.basename(file)
            headers.append(header)

    return groupby(headers, lambda h: h[field])
