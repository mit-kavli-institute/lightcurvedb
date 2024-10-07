import os
import pathlib
from glob import glob
from itertools import groupby, product

import click
from astropy.io import fits
from tabulate import tabulate


def extr_tic(filepath):
    return int(os.path.basename(filepath).split(".")[0])


def find_fits(*paths, **kwargs):
    allow_compressed = kwargs.get("allow_compressed", True)
    exts = ["*.fits.gz", "*.fits"] if allow_compressed else ["*.fits"]
    # To avoid duplication of frames strip out the file extensions and
    # keep track of which files we've seen (and skip).

    if allow_compressed:
        click.echo(
            click.style(
                "Allowing compressed FITS files. "
                "Reading these files will be orders "
                "of magnitude slower!",
                fg="yellow",
            )
        )

    # For now, preference to store compressed files
    history = set()
    fits_files = []
    # Order matters, check compressed files first
    for path, ext in product(paths, exts):
        files = glob(os.path.join(path, ext))
        for f in files:
            check = f.split(".")[0]
            if check in history:
                # We've already encountered this file
                continue
            history.add(check)
            fits_files.append(f)

    return fits_files


def find_h5(*paths):
    for path in paths:
        query = glob(os.path.join(path, "*.h5"))
        for result in query:
            yield result


def group_fits(files, field="ORBIT_ID"):
    headers = []
    with click.progressbar(files) as all_files:
        for file in all_files:
            header = dict(fits.getheader(file, 0))
            header["FILEPATH"] = file
            header["FILENAME"] = os.path.basename(file)
            headers.append(header)

    return groupby(headers, lambda h: h[field])


def group_h5(files):
    groups = groupby(sorted(files, key=extr_tic), key=extr_tic)
    for tic, files in groups:
        yield tic, list(files)


def slow_typecheck(string_param):
    """Attempt to interpret string into a core python type"""

    # Check to see if forcefully given string literal
    if string_param.startswith("'") and string_param.endswith("'"):
        return str(string_param)

    try:
        return int(string_param)
    except ValueError:
        # Not an integer
        pass
    try:
        return float(string_param)
    except ValueError:
        # Not a float
        pass

    # Default to resolve by string
    return str(string_param)


def resolve_filter_column(defined_columns, model, parameter):
    try:
        resolved = defined_columns[parameter]
    except KeyError:
        # see if value is on literal model field
        try:
            resolved = getattr(model, parameter)
        except AttributeError:
            # Must be literal scalar value
            # type check, could be a float, int or string
            resolved = slow_typecheck(parameter)

    return resolved


def tabulate_query(q, **tabulate_kwargs):
    """
    Provide a quick and dirty solution to pass an SQLAlchemy query into
    ``tabulate``.

    Returns
    -------
    str:
        The tabulated query.
    """
    query_info = q.column_descriptions
    results = q.all()

    column_names = [c["name"] for c in query_info]
    return tabulate(results, headers=column_names, **tabulate_kwargs)


def directory(exists=True):
    """
    A quick alias to click.Path to enforce pathlib usage, Restricted to
    allow only directories
    """
    return click.Path(file_okay=False, exists=exists, path_type=pathlib.Path)


def file(exists=True):
    """
    A quick alias to click.Path to enforce pathlib usage, Restricted to
    allow only files
    """
    return click.Path(dir_okay=False, exists=exists, path_type=pathlib.Path)
