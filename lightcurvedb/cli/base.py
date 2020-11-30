import click

from lightcurvedb.util.logger import (
    add_file_handler,
    add_stream_handler,
    set_level,
)

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
    help="If set to dryrun to permanent changes will be made to the database.",
)
@click.option("--logging", default="info", help="The global logging level.")
@click.option("--logfile", type=click.Path(dir_okay=False), help="Tee logging output to the provided filename.")
def lcdbcli(ctx, dbconf, dryrun, logging, logfile):
    """Master command for all lightcurve database commandline interaction"""
    add_stream_handler(logging)
    set_level(logging)

    if logfile:
        add_file_handler(logging, logfile)

    ctx.ensure_object(dict)
    ctx.obj["dryrun"] = dryrun
    ctx.obj["dbconf"] = dbconf
