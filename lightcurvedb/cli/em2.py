import pathlib
import tempfile

import click
from loguru import logger

from lightcurvedb.cli.base import lcdbcli
from lightcurvedb.core.ingestors import contexts
from lightcurvedb.core.ingestors import em2 as ingest_em2
from lightcurvedb.core.ingestors.jobs import EM2Plan


@lcdbcli.group()
@click.pass_context
def em2(ctx):
    pass


@em2.command()
@click.pass_context
@click.argument(
    "paths", nargs=-1, type=click.Path(file_okay=False, exists=True)
)
@click.option("--n-processes", default=16, type=click.IntRange(min=1))
@click.option("--recursive", "-r", is_flag=True, default=False)
@click.option(
    "--quality-flag-template",
    type=str,
    default=EM2Plan.DEFAULT_QUALITY_FLAG_TEMPLATE,
)
@click.option("--scratch", type=click.Path(file_okay=False, exists=True))
def ingest_dir(
    ctx,
    paths,
    n_processes,
    recursive,
    quality_flag_template,
    scratch,
):
    with tempfile.TemporaryDirectory(dir=scratch) as tempdir:
        tempdir_path = pathlib.Path(tempdir)
        cache_path = tempdir_path / "db.sqlite3"
        contexts.make_shared_context(cache_path)
        with ctx.obj["dbconf"] as db:
            contexts.populate_ephemeris(cache_path, db)
            contexts.populate_tjd_mapping(cache_path, db)

            directories = [pathlib.Path(path) for path in paths]
            for directory in directories:
                click.echo(f"Considering {directory}")

            plan = EM2Plan(directories, db, recursive=recursive)

            jobs = plan.get_jobs()
            tic_ids = plan.tic_ids
            contexts.populate_tic_catalog_w_db(cache_path, tic_ids)

            for args in plan.yield_needed_quality_flags(
                path_template=quality_flag_template
            ):
                logger.debug(f"Requiring quality flags {args}")
                contexts.populate_quality_flags(cache_path, *args)

        ingest_em2.ingest_jobs(
            ctx.obj["dbconf"],
            jobs,
            n_processes,
            cache_path,
            tempdir_path,
            log_level=ctx.obj["log_level"],
        )
        logger.success("Done!")
