from __future__ import division, print_function

import click

# from lightcurvedb import db_from_config
# from lightcurvedb.models import Lightcurve
# from lightcurvedb.managers import LightcurveManager
from lightcurvedb.cli.base import lcdbcli


@lcdbcli.group()
@click.pass_context
def plotting(ctx):
    pass


# @plotting.command()
# @click.pass_context
# @click.argument('tic', type=int)
# @click.options('--apertures', '-a', type=str, multiple=True)
# @click.options('--types', '-t', type=str, multiple=True)
# @click.options('--utf8', '-u', is_flag=True)
# def ascii(ctx, tic, apertures, types, utf8):
#     with ctx.obj['dbconf'] as db:
#         q = db.quer_lightcurves(
#             tics=[tic],
#             apertures=apertures,
#             types=types
#         )
#         lm = LightcurveManager.from_q(q)
#         lm.plot(ASCIIPlotter())
#
#
# @plotting.command()
# @click.pass_context
# @click.argument('tic', type=int)
# @click.options('--aperture', '-a', type=str, multiple=True)
# @click.options('--type', '-t', type=str, multiple=True)
# def curses(ctx, tic, apertures, types):
#     raise NotImplementedError
