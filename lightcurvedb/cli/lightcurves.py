from __future__ import division, print_function

from itertools import groupby, product, chain
from multiprocessing import Manager, Pool
from sqlalchemy import Sequence, text
from functools import partial
from collections import defaultdict

import click
import pandas as pd
import sys
import warnings
import os

from lightcurvedb.cli.base import lcdbcli
from lightcurvedb.cli.types import CommaList
from lightcurvedb.core.constants import CACHE_DIR
from lightcurvedb.core.ingestors.cache import IngestionCache
from lightcurvedb.core.ingestors.lightcurve_ingestors import load_lightpoints
from lightcurvedb.core.ingestors.lightpoint import (
    LightpointNormalizer,
    PartitionMerger,
    PartitionConsumer,
    PartitionJob,
    get_merge_jobs,
    ingest_merge_jobs,
)
from lightcurvedb.core.ingestors.temp_table import (
    FileObservation,
    QualityFlags,
    TIC8Parameters,
)
from lightcurvedb.legacy.timecorrect import (
    PartitionTimeCorrector,
    StaticTimeCorrector,
)
from lightcurvedb.core.datastructures.data_packers import (
    LightpointPartitionReader,
)
from lightcurvedb.models import (
    Lightcurve,
    Observation,
    Orbit,
    Lightpoint,
    Aperture,
    LightcurveType,
)
from lightcurvedb import models as defined_models
from tqdm import tqdm
from tabulate import tabulate
from lightcurvedb.util.logger import lcdb_logger as logger


def gaps_in_ids(id_array):
    """
    A naive slow approach to find missing numbers.
    """
    check_ids = set(id_array)

    start = min(check_ids)
    end = max(check_ids)

    ref = set(range(start, end + 1))

    return ref - check_ids


def ingest_by_tics(ctx, file_observations, tics, cache, n_processes, scratch):

    # Force uniqueness of TICs
    tics = set(map(int, tics))
    click.echo(
        "Will process {0} TICs".format(click.style(str(len(tics)), bold=True))
    )

    previously_seen = set()

    with ctx.obj["dbconf"] as db:
        m = Manager()
        job_queue = m.Queue(10000)
        normalizer = LightpointNormalizer(cache, db)
        time_corrector = StaticTimeCorrector(db.session)
        workers = []

        click.echo(
            "Grabbing current observation table to reduce duplicate work"
        )
        q = db.query(Observation.tic_id, Orbit.orbit_number).join(
            Observation.orbit
        )
        previously_seen = {(tic, orbit) for tic, orbit in q.all()}

        for _ in range(n_processes):
            producer = MassIngestor(
                db._config,
                quality_flags,
                time_corrector,
                job_queue,
                daemon=True,
            )
            workers.append(producer)

        click.echo(
            "Determining TIC job ordering to optimize PSQL cache hits..."
        )
        tic_q = (
            db.query(Lightcurve.tic_id)
            .filter(Lightcurve.tic_id.in_(tics))
            .order_by(Lightcurve.id)
            .distinct()
        )

        optimized_tics = []
        seen = set()
        for (tic,) in tic_q.all():
            if tic not in seen and tic in tics:
                optimized_tics.append(tic)
                seen.add(tic)

        click.echo(
            "Optimized {0} out of {1} tics".format(
                len(optimized_tics), len(tics)
            )
        )

    # Exit DB to remove idle session
    click.echo("Starting processes...")

    for process in workers:
        process.start()

    for tic in yield_optimized_tics(optimized_tics, tics):
        files = []

        df = file_observations[file_observations["tic_id"] == tic]

        for _, file_ in df.iterrows():
            tic, orbit, _, _, _ = file_
            if (file_.tic_id, file_.orbit_number) not in previously_seen:
                files.append(
                    (
                        file_.tic_id,
                        file_.orbit_number,
                        file_.camera,
                        file_.ccd,
                        file_.file_path,
                    )
                )

        if len(files) == 0:
            # Skip this work
            continue

        stellar_parameters = cache.session.query(TIC8Parameters).get(tic)

        if stellar_parameters is None:
            click.echo("Could not find parameters for {0}".format(tic))
            continue

        job = MergeJob(
            tic_id=tic,
            ra=stellar_parameters.right_ascension,
            dec=stellar_parameters.declination,
            tmag=stellar_parameters.tmag,
            file_observations=files,
        )
        job_queue.put(job)

    job_queue.join()
    click.echo("Merge-job queue empty")
    for process in workers:
        process.join()
        click.echo("Joined process {0}".format(process))

    click.echo(click.style("Done", fg="green", bold=True))


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
    with ctx.obj["dbconf"] as db:
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
