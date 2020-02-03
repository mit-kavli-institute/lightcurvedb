import click
from .types import Database

@click.group()
@click.option('--dbconf', type=Database(), help='Specify a database config for connections')
@click.option('--dryrun/--wetrun', default=False, help='If dryrun, no changes will be commited, recommended for first runs')
def lcdbcli(dbconf, dryrun):
    """Master command for all lightcurve database commandline interaction"""
    if dryrun:
        click.echo(click.style('Running in dryrun mode', fg='green'))
    else:
        click.echo(click.style('Running in wetrun mode!', fg='red'))

@lcdbcli.group()
@click.pass_context
def create(ctx):
    """Master command for all creation commands"""
    pass

@lcdbcli.group()
@click.pass_context
def ingest():
    """Master command for all ingest commands"""
    pass
