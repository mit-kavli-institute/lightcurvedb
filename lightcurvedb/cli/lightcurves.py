import click

from lightcurvedb.cli.base import lcdbcli


@lcdbcli.group()
@click.pass_context
def lightcurve(ctx):
    """
    Commands for ingesting and displaying lightcurves.
    """
    pass
