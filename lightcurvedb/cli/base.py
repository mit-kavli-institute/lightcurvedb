import click

from lightcurvedb.util.logger import (add_file_handler, add_stream_handler,
                                      set_level)

from .types import Database


@click.group()
@click.pass_context
@click.option(
    '--dbconf',
    default='~/.config/lightcurvedb/db.conf',
    type=Database(),
    help='Specify a database config for connections'
)
@click.option(
    '--dryrun/--wetrun',
    default=False,
    help='If dryrun, no changes will be commited, recommended for first runs'
)
@click.option(
    '--scratch',
    '-s',
    default='/scratch/tmp',
    type=click.Path(file_okay=False, exists=True),
    help='Path to scratch disk for caching'
)
@click.option(
    '--qlp-data',
    default='/pdo/qlp-data/',
    type=click.Path(file_okay=False, exists=True),
    help='The base QLP-Data directory'
)
@click.option('--logging', default='info')
@click.option('--logfile', type=click.Path(dir_okay=False))
def lcdbcli(ctx, dbconf, dryrun, scratch, qlp_data, logging, logfile):
    """Master command for all lightcurve database commandline interaction"""
    add_stream_handler(logging)
    set_level(logging)

    if logfile:
        add_file_handler(logging, logfile)

    ctx.ensure_object(dict)
    ctx.obj['dryrun'] = dryrun
    ctx.obj['dbconf'] = dbconf
    ctx.obj['scratch'] = scratch
    ctx.obj['qlp_data'] = qlp_data
