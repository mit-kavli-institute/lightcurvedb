from pathlib import Path

import click

from lightcurvedb.cli.base import lcdbcli
from lightcurvedb.core.ingestors.jobs import DirectoryPlan
from lightcurvedb.core.ingestors.lightpoints import ingest_merge_jobs


def gaps_in_ids(id_array):
    """
    A naive slow approach to find missing numbers.
    """
    check_ids = set(id_array)

    start = min(check_ids)
    end = max(check_ids)
    ref = set(range(start, end + 1))

    return ref - check_ids


@lcdbcli.group()
@click.pass_context
def lightcurve(ctx):
    """
    Commands for ingesting and displaying lightcurves.
    """


@lightcurve.command()
@click.pass_context
@click.argument(
    "paths", nargs=-1, type=click.Path(file_okay=False, exists=True)
)
@click.option("--n-processes", default=16, type=click.IntRange(min=1))
@click.option("--recursive", "-r", is_flag=True, default=False)
@click.option("--ingest/--plan", is_flag=True, default=True)
def ingest_dir(ctx, paths, n_processes, recursive, ingest):
    with ctx.obj["dbconf"] as db:
        directories = [Path(path) for path in paths]
        plan = DirectoryPlan(directories, db, recursive=recursive)
        jobs = plan.get_jobs()

    if ingest:
        ingest_merge_jobs(
            ctx.obj["dbconf"],
            jobs,
            n_processes,
            log_level=ctx.obj["log_level"],
        )
        click.echo("Done!")
    else:
        click.echo(plan)
