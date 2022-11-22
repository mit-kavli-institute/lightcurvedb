import click
from tabulate import tabulate

from lightcurvedb.models import Orbit

from . import lcdbcli


@lcdbcli.group()
@click.pass_context
def orbit(ctx):
    """Base Orbit Commands"""
    pass


@orbit.command()
@click.argument("orbit_numbers", nargs=-1, type=int)
@click.option("--parameter", "-p", multiple=True, type=Orbit.click_parameters)
@click.pass_context
def lookup(ctx, orbit_numbers, parameter):
    with ctx.obj["dbfactory"]() as db:
        cols = [getattr(Orbit, param) for param in parameter]
        q = db.query(*cols).filter(Orbit.orbit_number.in_(orbit_numbers))
        click.echo(tabulate(q.all(), headers=list(parameter)))


@orbit.command()
@click.argument("sectors", nargs=-1, type=int)
@click.option("--parameter", "-p", multiple=True, type=Orbit.click_parameters)
@click.pass_context
def sector_lookup(ctx, sectors, parameter):
    with ctx.obj["dbfactory"]() as db:
        cols = [getattr(Orbit, param) for param in parameter]
        q = db.query(*cols).filter(Orbit.sector.in_(sectors))
        click.echo(tabulate(q.all(), headers=list(parameter)))
