import click
import sys
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
@click.option("--logfile", type=click.Path(dir_okay=False))
def lcdbcli(ctx, dbconf, dryrun, logging, logfile):
    """Master command for all lightcurve database commandline interaction"""
    ctx.ensure_object(dict)
    if logging:
        logger.remove()
        logger.add(sys.stdout, level=logging.upper())
        logger.debug("Set logging to {0}".format(logging))

    ctx.obj["dryrun"] = dryrun
    ctx.obj["dbconf"] = dbconf
