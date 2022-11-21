import pathlib
import sys

import click
from loguru import logger

from lightcurvedb.core.connection import db_from_config

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
@click.option("--username-override", type=str)
@click.option("--db-name-override", type=str)
@click.option("--db-port-override", type=int)
@click.option("--db-host-override", type=str)
@click.option("--logging", type=str, default="info")
@click.option("--logfile", type=click.Path(dir_okay=False), default=None)
def lcdbcli(
    ctx,
    dbconf,
    dryrun,
    username_override,
    db_name_override,
    db_port_override,
    db_host_override,
    logging,
    logfile,
):
    """
    Master command for all lightcurve database commandline interaction
    """
    ctx.ensure_object(dict)
    logger.remove()
    if logfile is None:
        logger.add(sys.stdout, level=logging.upper())
    else:
        logger.add(logfile, level=logging.upper())
        ctx.obj["logfile"] = pathlib.Path(logfile)
    logger.debug(f"Set logging to {logging}")

    # Configure connection context for the cli script
    overrides = {}
    if username_override:
        overrides["username"] = username_override
    if db_name_override:
        overrides["database_name"] = db_name_override
    if db_host_override:
        overrides["database_host"] = db_host_override
    if db_port_override:
        overrides["database_port"] = db_port_override

    session = db_from_config(dbconf, **overrides)
    ctx.obj["db"] = session

    ctx.obj["log_level"] = logging
    ctx.obj["dryrun"] = dryrun
    ctx.obj["dbconf"] = dbconf
