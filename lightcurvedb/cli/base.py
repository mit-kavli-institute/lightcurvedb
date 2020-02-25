import click
import os
from .types import Database


@click.group()
@click.pass_context
@click.option('--dbconf', default=os.path.expanduser('~/.config/lightcurvedb/db.conf'), type=Database(), help='Specify a database config for connections')
@click.option('--dryrun/--wetrun', default=False, help='If dryrun, no changes will be commited, recommended for first runs')
@click.option('--scratch', '-s', default='/scratch/tmp', type=click.Path(file_okay=False, exists=True), help='Path to scratch disk for caching')
def lcdbcli(ctx, dbconf, dryrun, scratch):
    """Master command for all lightcurve database commandline interaction"""
    if dryrun:
        click.echo(click.style('Running in dryrun mode', fg='green'))
    else:
        click.echo(click.style('Running in wetrun mode!', fg='red'))
    ctx.ensure_object(dict)
    ctx.obj['dryrun'] = dryrun
    ctx.obj['dbconf'] = dbconf
    ctx.obj['scratch'] = scratch
