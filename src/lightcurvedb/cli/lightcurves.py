import multiprocessing as mp
import os
import pathlib
import tempfile
from functools import partial

import click
import sqlalchemy as sa
from loguru import logger
from tqdm import tqdm

from lightcurvedb import models as m
from lightcurvedb.cli.base import lcdbcli
from lightcurvedb.core.connection import db_from_config
from lightcurvedb.core.ingestors import contexts
from lightcurvedb.core.ingestors import lightcurve_arrays as ingest_em2_array
from lightcurvedb.core.ingestors.jobs import DirectoryPlan, TICListPlan
from lightcurvedb.core.procedures.remove_lightcurves import delete_lightcurves
from lightcurvedb.util.iter import chunkify


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
    "--quality-flag-template",
    type=str,
    default=DirectoryPlan.DEFAULT_QUALITY_FLAG_TEMPLATE,
    show_default=True,
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
    ctx.obj["n_processes"] = n_processes
    with tempfile.TemporaryDirectory(dir=scratch) as tempdir:
        tempdir_path = pathlib.Path(tempdir)
        cache_path = tempdir_path / "db.sqlite3"
        contexts.make_shared_context(cache_path)
        directories = [pathlib.Path(path) for path in paths]
        for directory in directories:
            logger.info(f"Considering {directory}")

        plan = DirectoryPlan(
            directories, ctx.obj["dbconf"], recursive=recursive
        )

        jobs = plan.get_jobs()

        for args in plan.yield_needed_quality_flags(
            path_template=quality_flag_template
        ):
            logger.debug(f"Requiring quality flags {args}")
            contexts.populate_quality_flags(cache_path, *args)

        ingest_em2_array.ingest_jobs(
            ctx.obj,
            jobs,
            cache_path,
        )
        logger.success("Done!")


@lightcurve.command()
@click.pass_context
@click.argument("tic_file", type=click.Path(dir_okay=False, exists=True))
@click.option("--n-processes", default=16, type=click.IntRange(min=1))
@click.option(
    "--quality-flag-template",
    type=str,
    default=DirectoryPlan.DEFAULT_QUALITY_FLAG_TEMPLATE,
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
    ctx.obj["n_processes"] = n_processes
    with tempfile.TemporaryDirectory(dir=scratch) as tempdir:
        cache_path = pathlib.Path(tempdir, "db.sqlite3")
        contexts.make_shared_context(cache_path)

        plan = TICListPlan(tic_ids, ctx.obj["dbconf"])
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
            ctx.obj,
            jobs,
            cache_path,
        )
        click.echo("Done!")


@lightcurve.command()
@click.pass_context
@click.argument("orbit-number", type=int)
@click.option("--n-workers", "-n", type=int, default=os.cpu_count())
def remove_lightcurves(ctx, orbit_number: int, n_workers: int):
    with db_from_config(ctx.obj["dbconf"]) as db:
        q = (
            sa.select(m.BestOrbitLightcurve.tic_id)
            .join(m.BestOrbitLightcurve.orbit)
            .where(m.Orbit.orbit_number == orbit_number)
        )
        tic_ids = list(db.scalars(q))

    click.echo(f"Will remove {len(tic_ids)} stars in orbit {orbit_number}")
    click.confirm("Irreversable command, confirm action", abort=True)
    chunks = list(chunkify(tic_ids, 1000))
    func = partial(delete_lightcurves, ctx.obj["dbconf"], orbit_number)

    with mp.Pool(n_workers) as pool, tqdm(total=len(tic_ids)) as tbar:
        if "logfile" not in ctx.obj:
            # If logging to standard out, we need to ensure loguru
            # does not step over tqdm output.
            logger.remove()
            logger.add(
                lambda msg: tqdm.write(msg, end=""),
                colorize=True,
                level=ctx.obj["log_level"].upper(),
                enqueue=True,
            )
        results = pool.imap_unordered(func, chunks)
        for n_removed in results:
            tbar.update(n_removed)
