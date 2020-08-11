from __future__ import division, print_function
import click
import pandas as pd
import re
from sys import exit
from collections import OrderedDict
from lightcurvedb.cli.base import lcdbcli
from lightcurvedb.models import Lightcurve


QFLAG_DTYPE = OrderedDict(cadence=int, quality_flag=int)

QFILE_PATTERN = re.compile(
    r'cam(?P<camera>[1-4])ccd(?P<ccd>[1-4])_qflag\.txt$'
)


def qflag_to_df(filepath):
    click.echo('Processing {}'.format(click.style(filepath, bold=True)))
    df = pd.read_csv(
        filepath, delimiter=' ', names=QFLAG_DTYPE.keys(), dtype=QFLAG_DTYPE
    )
    return df


def qflag_components(filepath):
    match = QFILE_PATTERN.search(filepath)
    if match is None:
        click.echo(
            'File does not match the needed pattern {}'.format(
                click.style(QFILE_PATTERN.pattern, bold=True, fg='white')
            )
        )
        exit(1)
    result = match.groupdict()
    return int(result['camera']), int(result['ccd'])


@lcdbcli.group()
@click.pass_context
def quality_flags(ctx):
    pass


@quality_flags.command()
@click.pass_context
@click.argument('orbit', type=int)
@click.argument(
    'quality_flag_files',
    nargs=-1,
    type=click.Path(dir_okay=False, exists=True),
)
def ingest_files(ctx, orbit, quality_flag_files):
    """
    Ingest the QUALITY_FLAGS_FILES specified with lightcurves observed in the
    given ORBIT.

    Notes
    -----
    This function utilizes temporary tables which will not allow a --dryrun
    to be utilized. Proceed with caution
    """

    if ctx.obj['dryrun']:
        click.echo(
            'Function does not support {}!'.format(
                click.style('--dryrun', bold=True, fg='red', blink=True)
            )
        )
        exit(1)  # Exit as non-zero

    with ctx.obj['dbconf'] as db:
        for qflag_file in quality_flag_files:
            camera, ccd = qflag_components(qflag_file)
            df = qflag_to_df(qflag_file)
            db.set_quality_flags(
                orbit, camera, ccd, df.cadences, df.quality_flags
            )
            click.echo(
                'Set {} quality flags using {}'.format(
                    len(df), click.style(qflag_file, bold=True, fg='green')
                )
            )
