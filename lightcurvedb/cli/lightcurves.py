from __future__ import division, print_function

import click
from tabulate import tabulate

from lightcurvedb.cli.base import lcdbcli
from lightcurvedb.cli.types import CommaList
from lightcurvedb.models import Lightcurve
from lightcurvedb.core.datastructures.data_packers import (
    LightpointPartitionReader,
)
from lightcurvedb.core.ingestors.cache import IngestionCache
from lightcurvedb.core.ingestors.jobs import IngestionPlan
from lightcurvedb.core.ingestors.lightpoint import ingest_merge_jobs


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
    pass


@lightcurve.command()
@click.pass_context
@click.argument("orbits", type=int, nargs=-1)
@click.option("--n-processes", default=16, type=click.IntRange(min=1))
@click.option("--cameras", type=CommaList(int), default="1,2,3,4")
@click.option("--ccds", type=CommaList(int), default="1,2,3,4")
@click.option("--fill-id-gaps", "fillgaps", is_flag=True, default=False)
def ingest_h5(ctx, orbits, n_processes, cameras, ccds, fillgaps):

    cache = IngestionCache()
    with ctx.obj["dbconf"] as db:
        plan = IngestionPlan(
            db, cache, orbits=orbits, cameras=cameras, ccds=ccds
        )
        click.echo(plan)
        plan.assign_new_lightcurves(db, fill_id_gaps=fillgaps)

    jobs = plan.get_jobs_by_partition()

    ingest_merge_jobs(
        ctx.obj["dbconf"]._config, jobs, n_processes, not ctx.obj["dryrun"]
    )
    click.echo("Done!")


@lightcurve.command()
@click.pass_context
@click.argument("tics", type=int, nargs=-1)
@click.option("--n-processes", default=1, type=click.IntRange(min=1))
@click.option("--fill-id-gaps", "fillgaps", is_flag=True, default=False)
def ingest_tic(ctx, tics, n_processes, fillgaps):
    cache = IngestionCache()
    with ctx.obj["dbconf"] as db:
        plan = IngestionPlan(db, cache, tic_mask=tics)
        click.echo(plan)
        plan.assign_new_lightcurves(db, fill_id_gaps=fillgaps)

    jobs = plan.get_jobs_by_partition()

    ingest_merge_jobs(
        ctx.obj["dbconf"]._config, jobs, n_processes, not ctx.obj["dryrun"]
    )
    click.echo("Done!")


@lightcurve.command()
@click.pass_context
@click.argument("orbits", type=int, nargs=-1)
@click.option("--cameras", type=CommaList(int), default="1,2,3,4")
@click.option("--ccds", type=CommaList(int), default="1,2,3,4")
def view_orbit_ingestion_plan(ctx, orbits, cameras, ccds):
    cache = IngestionCache()
    with ctx.obj["dbconf"] as db:
        plan = IngestionPlan(
            db,
            cache,
            orbits=orbits,
            cameras=cameras,
            ccds=ccds,
        )
    click.echo(plan)


@lightcurve.command()
@click.pass_context
@click.argument("tic_ids", type=int, nargs=-1)
@click.option("--cameras", type=CommaList(int), default="1,2,3,4")
@click.option("--ccds", type=CommaList(int), default="1,2,3,4")
def view_tic_ingestion_plan(ctx, tic_ids, cameras, ccds):
    cache = IngestionCache()
    with ctx.obj["dbconf"] as db:
        plan = IngestionPlan(
            db, cache, cameras=cameras, ccds=ccds, tic_mask=tic_ids
        )
    click.echo(plan)


@lightcurve.command()
@click.pass_context
@click.argument("lightcurve_ids", type=int, nargs=-1)
def view_lightcurve_id_ingestion_plan(ctx, lightcurve_ids):
    cache = IngestionCache()
    with ctx.obj["dbconf"] as db:
        tics = db.query(Lightcurve.tic_id).filter(
            Lightcurve.id.in_(lightcurve_ids)
        )
        plan = IngestionPlan(db, cache, tic_mask={tic for tic, in tics})
    click.echo(plan)


@lightcurve.group()
@click.pass_context
@click.argument("blob_path", type=click.Path(dir_okay=False, exists=True))
def blob(ctx, blob_path):
    ctx.obj["blob_path"] = blob_path


@blob.command()
@click.pass_context
def print_observations(ctx):
    with ctx.obj["dbconf"] as db:
        reader = LightpointPartitionReader(ctx.obj["blob_path"])
        click.echo(reader.print_observations(db))


@blob.command()
@click.pass_context
@click.option(
    "--parameters", "-p", multiple=True, default=["lightcurve_id", "cadence"]
)
def print_lightpoints(ctx, parameters):
    with ctx.obj["dbconf"]:
        reader = LightpointPartitionReader(ctx.obj["blob_path"])
        click.echo(
            tabulate(
                list(reader.yield_lightpoints(*parameters)),
                headers=parameters,
                floatfmt=".4f",
            )
        )


@blob.command()
def print_summary(ctx):
    reader = LightpointPartitionReader(ctx.obj["blob_path"])
    table = reader.print_summary(ctx.obj["dbconf"])
    click.echo(table)
