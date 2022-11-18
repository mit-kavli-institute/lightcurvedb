import pathlib
import tempfile

import click
from loguru import logger

from lightcurvedb.cli.base import lcdbcli
from lightcurvedb.core.ingestors import contexts
from lightcurvedb.core.ingestors import em2_array as ingest_em2_array
from lightcurvedb.core.ingestors.jobs import EM2_ArrayTICListPlan, EM2Plan


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
@click.option("--tic-catalog/--tic-db", is_flag=True, default=True)
@click.option(
    "--tic-catalog-path-template",
    type=str,
    default=EM2Plan.DEFAULT_TIC_CATALOG_TEMPLATE,
    show_default=True,
)
@click.option(
    "--quality-flag-template",
    type=str,
    default=EM2Plan.DEFAULT_QUALITY_FLAG_TEMPLATE,
    show_default=True,
)
@click.option("--scratch", type=click.Path(file_okay=False, exists=True))
def ingest_dir(
    ctx,
    paths,
    n_processes,
    recursive,
    tic_catalog,
    tic_catalog_path_template,
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
                logger.info(f"Considering {directory}")

            plan = EM2Plan(directories, db, recursive=recursive)

            jobs = plan.get_jobs()
            if tic_catalog:
                path_iter = plan.yield_needed_tic_catalogs(
                    path_template=tic_catalog_path_template
                )
                for catalog_path in path_iter:
                    contexts.populate_tic_catalog(cache_path, catalog_path)
            else:
                tic_ids = plan.tic_ids
                contexts.populate_tic_catalog_w_db(cache_path, tic_ids)

            for args in plan.yield_needed_quality_flags(
                path_template=quality_flag_template
            ):
                logger.debug(f"Requiring quality flags {args}")
                contexts.populate_quality_flags(cache_path, *args)

        ingest_em2_array.ingest_jobs(
            ctx.obj["dbconf"],
            jobs,
            n_processes,
            cache_path,
            log_level=ctx.obj["log_level"],
        )
        logger.success("Done!")


@lightcurve.command()
@click.pass_context
@click.argument("tic_file", type=click.Path(dir_okay=False, exists=True))
@click.option("--n-processes", default=16, type=click.IntRange(min=1))
@click.option(
    "--quality-flag-template",
    type=str,
    default=EM2Plan.DEFAULT_QUALITY_FLAG_TEMPLATE,
)
@click.option("--scratch", type=click.Path(file_okay=False, exists=True))
@click.option("--fill-id-gaps", is_flag=True)
def ingest_tic_list(
    ctx,
    tic_file,
    n_processes,
    quality_flag_template,
    scratch,
    fill_id_gaps,
):
    tic_ids = set(map(int, open(tic_file, "rt").readlines()))
    with tempfile.TemporaryDirectory(dir=scratch) as tempdir:
        cache_path = pathlib.Path(tempdir, "db.sqlite3")
        contexts.make_shared_context(cache_path)
        with ctx.obj["dbconf"] as db:
            contexts.populate_ephemeris(cache_path, db)
            contexts.populate_tjd_mapping(cache_path, db)

            plan = EM2_ArrayTICListPlan(tic_ids, db)
            if fill_id_gaps:
                plan.fill_id_gaps()
            jobs = plan.get_jobs()

            contexts.populate_tic_catalog_w_db(cache_path, tic_ids)

            for args in plan.yield_needed_quality_flags(
                path_template=quality_flag_template
            ):
                logger.debug(f"Requiring quality flags {args}")
                contexts.populate_quality_flags(cache_path, *args)

        ingest_em2_array.ingest_jobs(
            ctx.obj["dbconf"],
            jobs,
            n_processes,
            cache_path,
            log_level=ctx.obj["log_level"],
        )
        click.echo("Done!")
