from __future__ import division, print_function

from itertools import groupby, product, chain
from multiprocessing import Manager, Pool
from sqlalchemy import Sequence, text
from pgcopy import CopyManager
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
    MassIngestor,
    MergeJob,
    SingleMergeJob,
    partition_copier,
    LightpointNormalizer,
    PartitionMerger,
    PartitionConsumer,
    PartitionJob,
    get_jobs,
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
from lightcurvedb.models import Lightcurve, Observation, Orbit, Lightpoint, Aperture, LightcurveType
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

    ref = set(range(start, end+1))

    return ref - check_ids


def get_merge_jobs(ctx, cache, orbits, cameras, ccds, fillgaps=False):
    """
    Get a list of SingleMergeJobs from TICs appearing in the
    given orbits, cameras, and ccds. TICs and orbits already ingested
    will *not* be in the returned list.

    In addition, any new Lightcurves will be assigned IDs so it will
    be deterministic in which partition its data will reside in.
    """
    cache_q = cache.session.query(
        FileObservation.tic_id,
        FileObservation.orbit_number,
        FileObservation.file_path
    )
    
    if not all(cam in cameras for cam in [1,2,3,4]):
        cache_q = cache_q.filter(FileObservation.camera.in_(cameras))
    if not all(ccd in ccds for ccd in [1,2,3,4]):
        cache_q = cache_q.filter(FileObservation.ccd.in_(ccds))

    file_df = pd.read_sql(
        cache_q.statement,
        cache.session.bind,
        index_col=["tic_id"]
    )

    relevant_tics = set(
        file_df[file_df.orbit_number.isin(orbits)].index
    )

    file_df = file_df.loc[relevant_tics].sort_index()

    obs_clause = (
        Orbit.orbit_number.in_(orbits),
        Observation.camera.in_(cameras),
        Observation.ccd.in_(ccds),
    )
    click.echo("Comparing cache file paths to lcdb observation table")
    with ctx.obj["dbconf"] as db:
        obs_q = db.query(
            Observation.tic_id,
            Orbit.orbit_number
        ).join(
            Observation.orbit       
        ).filter(
            *obs_clause
        )
        apertures = [ap.name for ap in db.query(Aperture)]
        types = [t.name for t in db.query(LightcurveType)]
        already_observed = set(obs_q)

        click.echo("Preparing lightcurve id map")
        lcs = db.lightcurves.filter(
            Lightcurve.tic_id.in_(
                db.query(
                    Observation.tic_id
                ).join(Observation.orbit).filter(
                    *obs_clause
                ).distinct().subquery()
            )
        ).all()
        lc_id_map = {
            (lc.tic_id, lc.aperture_id, lc.lightcurve_type_id): lc.id
            for lc in lcs
        }
        jobs = get_jobs(
            db,
            file_df,
            already_observed,
            apertures,
            types,
            fill_id_gaps=fillgaps,
            bar=tqdm
        )
    return jobs


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


    click.echo(click.style("Done", fg="green", bold=True))


def ingest_merge_jobs(config, merge_jobs, n_processes, commit):
    """
    """
    # Group each merge_job
    bucket = defaultdict(list)
    for merge_job in merge_jobs:
        partition_id = (merge_job.id // 1000) * 1000
        bucket[partition_id].append(merge_job)
    click.echo(
        "{0} partitions will be affected".format(len(bucket))
    )

    with db_from_config(config) as db:
        normalizer = LightpointNormalizer(db)

    func = partial(
        copy_lightpoints,
        db._config,
        normalizer,
        commit=commit
    )

    total_jobs = len(bucket)

    err = click.style("ERROR", bg="red", blink=True)
    ok = click.style("OK", fg="green", bold=True)

    error_msg = (
        "{0}: Could not open {{0}} files. "
        "List written to {{1}}".format(
            err
        )
    )
    ok_msg = (
        "{0}: Copied {{0}} files. "
        "Merge time {{1}}s. "
        "Validation time {{2}}s. "
        "Copy time {{3}}s".format(
            ok
        )
    )
    all_results = []
    click.echo("Sending work to {0} processes".format(n_processes))
    with Pool(n_processes) as pool:
        results = pool.imap_unordered(func, bucket.values())
        bar = tqdm(total=total_jobs)
        for r in results:
            if r["missed_files"]:
                with open("./missed_merges.txt", "at") as out:
                    out.write("\n".join(r["missed_files"]))

            if r["status"] == "ERROR":
                path = os.path.abspath("./missed_merges.txt")
                msg = error_msg.format(
                    len(r["missed_files"]),
                    path
                )
            elif r["STATUS"] == "OK":
                msg = ok_msg.format(
                    len("n_files"),
                    r["merge_elapsed"],
                    r["validation_elapsed"],
                    r["copy_elapsed"]
                )
            bar.write(msg)
            all_results.append(r)
            bar.update(1)


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
    jobs = get_merge_jobs(
        ctx,
        cache,
        orbits,
        cameras,
        ccds,
        fillgaps=fillgaps)
    click.echo("Obtained {0} jobs to perform".format(len(jobs)))

    ingest_merge_jobs(
        ctx.obj["dbconf"]._config,
        jobs,
        n_processes,
        not ctx.obj["dryrun"]
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
@click.pass_context
def print_summary(ctx):
    reader = LightpointPartitionReader(ctx.obj["blob_path"])
    table = reader.print_summary(ctx.obj["dbconf"])
    click.echo(table)
