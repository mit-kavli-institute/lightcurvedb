from __future__ import division, print_function

import click
import pandas as pd
from tabulate import tabulate

from lightcurvedb.cli.base import lcdbcli
from lightcurvedb.cli.types import CommaList
from lightcurvedb.core.datastructures.data_packers import (
    LightpointPartitionReader,
)
from lightcurvedb.core.ingestors.cache import IngestionCache
from lightcurvedb.core.ingestors.lightpoint import (
    get_merge_jobs,
    get_jobs_by_tic,
    ingest_merge_jobs,
)
from lightcurvedb.core.ingestors.lightcurve_ingestors import get_ingestion_plan


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
    click.echo("Connected to ingestion cache, determining filepaths")
    jobs = list(
        get_merge_jobs(ctx, cache, orbits, cameras, ccds, fillgaps=fillgaps)
    )
    click.echo("Obtained {0} jobs to perform".format(len(jobs)))

    ingest_merge_jobs(
        ctx.obj["dbconf"]._config, jobs, n_processes, not ctx.obj["dryrun"]
    )
    click.echo("Done!")


@lightcurve.command()
@click.pass_context
@click.argument("tics", type=int, nargs=-1)
@click.option("--only-orbit", "-o", "orbits", type=int, multiple=True)
@click.option("--aperture", "-a", "apertures", type=str, multiple=True)
@click.option("--type", "-t", "types", type=str, multiple=True)
@click.option("--n-processes", default=16, type=click.IntRange(min=1))
@click.option("--fill-id-gaps", "fillgaps", is_flag=True, default=False)
def ingest_tic(ctx, tics, orbits, apertures, types, n_processes, fillgaps):
    if not tics:
        click.echo("No tic ids passed...")
        return
    cache = IngestionCache()
    click.echo("Connected to ingestion cache, determining filepaths")
    jobs = list(
        get_jobs_by_tic(
            ctx,
            cache,
            tics,
            fillgaps=fillgaps,
            orbits=orbits,
            apertures=apertures,
            types=types,
        )
    )
    click.echo("Obtained {0} jobs to perform".format(len(jobs)))
    ingest_merge_jobs(
        ctx.obj["dbconf"]._config, jobs, n_processes, not ctx.obj["dryrun"]
    )


@lightcurve.command()
@click.pass_context
@click.argument("orbits", type=int, nargs=-1)
@click.option("--cameras", type=CommaList(int), default="1,2,3,4")
@click.option("--ccds", type=CommaList(int), default="1,2,3,4")
def view_orbit_ingestion_plan(ctx, orbits, cameras, ccds):
    cache = IngestionCache()
    with ctx.obj["dbconf"] as db:
        plan = get_ingestion_plan(
            db, cache, orbits=orbits, cameras=cameras, ccds=ccds
        )
        df = pd.DataFrame(obs.to_dict for obs in plan)

    try:
        grouped_summary = df.groupby(["orbit_number", "camera", "ccd"]).size()
        click.echo("Orbital Summary")
        click.echo(grouped_summary)
    except KeyError:
        click.echo(df)


@lightcurve.command()
@click.pass_context
@click.argument("tic_ids", type=int, nargs=-1)
@click.option("--cameras", type=CommaList(int), default="1,2,3,4")
@click.option("--ccds", type=CommaList(int), default="1,2,3,4")
def view_tic_ingestion_plan(ctx, tic_ids, cameras, ccds):
    cache = IngestionCache()
    with ctx.obj["dbconf"] as db:
        plan = get_ingestion_plan(
            db, cache, tic_mask=tic_ids, cameras=cameras, ccds=ccds
        )
        df = pd.DataFrame(obs.to_dict for obs in plan)

    try:
        click.echo("TIC Plan Summary")
        click.echo(df)
    except KeyError:
        click.echo(df)


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
