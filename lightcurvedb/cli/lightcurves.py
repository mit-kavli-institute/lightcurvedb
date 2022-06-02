import tempfile
from pathlib import Path

import click
from loguru import logger

from lightcurvedb.cli.base import lcdbcli
from lightcurvedb.core.ingestors import contexts
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
@click.option(
    "--tic-catalog-template",
    type=str,
    default=DirectoryPlan.DEFAULT_TIC_CATALOG_TEMPLATE,
)
@click.option(
    "--quality-flag-template",
    type=str,
    default=DirectoryPlan.DEFAULT_QUALITY_FLAG_TEMPLATE,
)
def ingest_dir(
    ctx,
    paths,
    n_processes,
    recursive,
    tic_catalog_template,
    quality_flag_template,
):
    with tempfile.TemporaryDirectory() as tempdir:
        cache_path = Path(tempdir, "db.sqlite3")
        contexts.make_shared_context(cache_path)
        with ctx.obj["dbconf"] as db:
            contexts.populate_ephemeris(cache_path, db)
            contexts.populate_tjd_mapping(cache_path, db)

            directories = [Path(path) for path in paths]
            for directory in directories:
                click.echo(f"Considering {directory}")

            plan = DirectoryPlan(directories, db, recursive=recursive)
            jobs = plan.get_jobs()

            for catalog in plan.yield_needed_tic_catalogs(
                path_template=tic_catalog_template
            ):
                logger.debug(f"Requiring catalog {catalog}")
                contexts.populate_tic_catalog(cache_path, catalog)

            for args in plan.yield_needed_quality_flags(
                path_template=quality_flag_template
            ):
                logger.debug(r"Requiring qualit flags {args}")
                contexts.populate_quality_flags(cache_path, *args)

        ingest_merge_jobs(
            ctx.obj["dbconf"],
            jobs,
            n_processes,
            cache_path,
            log_level=ctx.obj["log_level"],
        )
        click.echo("Done!")
