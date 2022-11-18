import pathlib
import sys

import click
from loguru import logger

from .types import Database


@click.group()
@click.pass_context
@click.option(
    "--dbconf",
    default="~/.config/lightcurvedb/db.conf",
    type=Database(),
    help="Specify a database config for connections",
)
@click.option(
    "--dryrun/--wetrun",
    default=False,
    help="If dryrun, no changes will be commited, recommended for first runs",
)
@click.option("--logging", type=str, default="info")
@click.option("--logfile", type=click.Path(dir_okay=False), default=None)
def lcdbcli(ctx, dbconf, dryrun, logging, logfile):
    """Master command for all lightcurve database commandline interaction"""
    ctx.ensure_object(dict)
    logger.remove()
    if logfile is None:
        logger.add(sys.stdout, level=logging.upper())
    else:
        logger.add(logfile, level=logging.upper())
        ctx.obj["logfile"] = pathlib.Path(logfile)

    logger.debug(f"Set logging to {logging}")

    ctx.obj["log_level"] = logging
    ctx.obj["dryrun"] = dryrun
    ctx.obj["dbconf"] = dbconf
