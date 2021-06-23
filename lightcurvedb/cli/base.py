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
    help="If dryrun, no changes will be commited, recommended for first runs",
)
@click.option("--logging", type=str, default="info")
@click.option("--logfile", type=click.Path(dir_okay=False))
def lcdbcli(ctx, dbconf, dryrun, logging, logfile):
    """Master command for all lightcurve database commandline interaction"""

    ctx.ensure_object(dict)
    add_stream_handler(logging)
    ctx.obj["dryrun"] = dryrun
    ctx.obj["dbconf"] = dbconf
