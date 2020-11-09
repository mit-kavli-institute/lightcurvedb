from __future__ import division, print_function

from itertools import groupby, product
from multiprocessing import Manager, Pool
from sqlalchemy import Sequence
from pgcopy import CopyManager

import click
import pandas as pd

from lightcurvedb.cli.base import lcdbcli
from lightcurvedb.cli.types import CommaList
from lightcurvedb.core.constants import CACHE_DIR
from lightcurvedb.core.ingestors.cache import IngestionCache
from lightcurvedb.core.ingestors.lightpoint import (
    MassIngestor,
    MergeJob,
    SingleMergeJob,
    partition_copier,
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
    LightpointPartitionReader
)
from lightcurvedb.models import Lightcurve, Observation, Orbit
from tqdm import tqdm

INGESTION_MODES = ["smart", "ignore", "full"]

IngestionMode = click.Choice(INGESTION_MODES)


def yield_optimized_tics(ordered_tics, all_tics):
    for tic in ordered_tics:
        try:
            all_tics.remove(tic)
            yield tic
        except KeyError:
            continue
    for tic in all_tics:
        yield tic


def partition_id(id_):
    return (id_ // 1000) * 1000


def ingest_by_tics(ctx, file_observations, tics, cache, n_processes, scratch):

    # Force uniqueness of TICs
    tics = set(map(int, tics))

    quality_flags = pd.read_sql(
        cache.session.query(
            QualityFlags.cadence.label("cadences"),
            QualityFlags.camera,
            QualityFlags.ccd,
            QualityFlags.quality_flag.label("quality_flags"),
        ).statement,
        cache.session.bind,
        index_col=["cadences", "camera", "ccd"],
    )

    click.echo(
        "Will process {0} TICs".format(click.style(str(len(tics)), bold=True))
    )

    previously_seen = set()

    with ctx.obj["dbconf"] as db:
        m = Manager()
        job_queue = m.Queue(10000)
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
        tic_q = db.query(Lightcurve.tic_id).order_by(Lightcurve.id).distinct()

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
@click.option("--n-processes", default=48, type=click.IntRange(min=1))
@click.option("--cameras", type=CommaList(int), default="1,2,3,4")
@click.option("--ccds", type=CommaList(int), default="1,2,3,4")
@click.option(
    "--scratch",
    type=click.Path(exists=True, file_okay=False),
    default=CACHE_DIR,
)
@click.option("--update-type", type=IngestionMode, default=INGESTION_MODES[0])
def ingest_h5(ctx, orbits, n_processes, cameras, ccds, scratch, update_type):

    cache = IngestionCache()
    click.echo("Connected to ingestion cache, determining filepaths")
    tic_subq = (
        cache.session.query(FileObservation.tic_id)
        .filter(
            FileObservation.orbit_number.in_(orbits),
            FileObservation.camera.in_(cameras),
            FileObservation.ccd.in_(ccds),
        )
        .distinct(FileObservation.tic_id)
    )

    file_obs_q = cache.session.query(
        FileObservation.tic_id,
        FileObservation.orbit_number,
        FileObservation.camera,
        FileObservation.ccd,
        FileObservation.file_path,
    ).filter(FileObservation.tic_id.in_(tic_subq.subquery()))

    df = pd.read_sql(file_obs_q.statement, cache.session.bind)

    ingest_by_tics(ctx, df, df.tic_id, cache, n_processes, scratch)


@lightcurve.command()
@click.pass_context
@click.argument("tics", type=int, nargs=-1)
@click.option("--n-processes", default=48, type=click.IntRange(min=1))
@click.option(
    "--scratch",
    type=click.Path(exists=True, file_okay=False),
    default=CACHE_DIR,
)
def manual_ingest(ctx, tics, n_processes, scratch):
    cache = IngestionCache()
    click.echo("Connected to ingestion cache, determining filepaths")

    file_obs_q = cache.session.query(
        FileObservation.tic_id,
        FileObservation.orbit_number,
        FileObservation.camera,
        FileObservation.ccd,
        FileObservation.file_path,
    )
    file_observations = pd.read_sql(file_obs_q.statement, cache.session.bind)

    ingest_by_tics(ctx, file_observations, tics, cache, n_processes, scratch)


@lightcurve.command()
@click.pass_context
@click.argument("tic_file", type=click.Path(exists=True, dir_okay=False))
@click.option("--n-processes", default=48, type=click.IntRange(min=1))
@click.option(
    "--scratch",
    type=click.Path(exists=True, file_okay=False),
    default=CACHE_DIR,
)
def tic_list(ctx, tic_file, n_processes, scratch):
    tics = {int(tic.strip()) for tic in open(tic_file, "rt").readlines()}
    cache = IngestionCache()
    click.echo("Connected to ingestion cache, determining filepaths")

    file_obs_q = cache.session.query(
        FileObservation.tic_id,
        FileObservation.orbit_number,
        FileObservation.camera,
        FileObservation.ccd,
        FileObservation.file_path,
    )
    file_observations = pd.read_sql(file_obs_q.statement, cache.session.bind)

    ingest_by_tics(ctx, file_observations, tics, cache, n_processes, scratch)


@lightcurve.command()
@click.pass_context
@click.argument("orbits", nargs=-1, type=int)
@click.option("--cameras", type=CommaList(int), default="1,2,3,4")
@click.option("--ccds", type=CommaList(int), default="1,2,3,4")
@click.option("--n-processes", default=48, type=click.IntRange(min=1))
@click.option("--update/--no-update", default=False)
@click.option(
    "--types",
    type=str,
    multiple=True,
    default=["KSPMagnitude", "RawMagnitude"],
)
@click.option(
    "--apertures",
    type=str,
    multiple=True,
    default=["Aperture_00{0}".format(xth) for xth in range(5)],
)
def partition_ingest(
    ctx, orbits, cameras, ccds, n_processes, types, apertures, update
):
    cache = IngestionCache()
    tic_q = cache.session.query(FileObservation.tic_id).filter(
        FileObservation.orbit_number.in_(orbits),
        FileObservation.camera.in_(cameras),
        FileObservation.ccd.in_(ccds),

    ).distinct()

    file_observation_q = cache.session.query(FileObservation).filter(
        FileObservation.tic_id.in_(tic_q.subquery())
    )

    click.echo("Loading in h5 location maps from cache...")

    cache_df = pd.read_sql(
        file_observation_q.statement, cache.session.bind, index_col=["tic_id"]
    )

    click.echo("Loading relevant TIC parameters from cache...")

    tic_parameters_q = cache.session.query(
        TIC8Parameters.tic_id,
        TIC8Parameters.tmag,
        TIC8Parameters.right_ascension,
        TIC8Parameters.declination,
    )
    tic_parameters = pd.read_sql(
        tic_parameters_q.statement, cache.session.bind, index_col=["tic_id"]
    )

    click.echo("Loading quality flags from cache...")
    quality_flags = cache.quality_flag_df

    full_length = len(cache_df)
    click.echo(
        "determined {0} files to be processed".format(
            full_length
        )
    )
    cache.session.close()

    with ctx.obj["dbconf"] as db:
        # Remove jobs already ingested
        click.echo("Connected to lightcurve database")
        orbit_map = {
            number: id_
            for number, id_ in db.query(Orbit.orbit_number, Orbit.id)
        }
        if not update:
            click.echo("\tDetermining existing observations...")
            existing_obs = set(
                db.query(Observation.tic_id, Orbit.orbit_number)
                .join(Observation.orbit)
                .filter(
                    Observation.camera.in_(cameras),
                    Observation.ccd.in_(ccds)
                ).yield_per(1000)
            )
            click.echo(
                "\tFlagging duplicates..."
            )
            cache_df["already_ingested"] = cache_df.apply(
                lambda row: (int(row.name), row["orbit_number"]) in existing_obs,
                axis=1,
            )


            click.echo("\tRemoving ingestions that already exist...")
            new_obs = cache_df[~cache_df["already_ingested"]]
            new_length = len(new_obs)

            if (full_length - new_length) > 0:
                n_removed = full_length - new_length
                click.echo(
                    "\tRemoved {0} ingestion jobs to a total of {1}".format(
                        n_removed,
                        new_length
                    )
                )
        else:
            new_obs = cache_df

        tics = set(new_obs.reset_index()['tic_id'].values)

        click.echo("\tPreparing time corrector")
        time_corrector = PartitionTimeCorrector(db.session)

        click.echo("\tMaking lightcurve paramters -> id map")
        lc_id_q = db.lightcurve_id_map(resolve=False).filter(
            Lightcurve.tic_id.in_(tics)
        )

        id_map = {
            (tic_id, ap, lct): id_
            for id_, tic_id, ap, lct in lc_id_q.yield_per(1000)
        }

        click.echo("Assigning IDs to expected new lightcurves...")
        # Pre-assign ids, and group by destination partition
        new_lc_params = []
        jobs = []
        id_seq = Sequence("lightcurves_id_seq")

        expected_len = len(tics) * len(types) * len(apertures)
        prod_iter = product(tics, types, apertures)

        for tic_id, type_, aperture in tqdm(prod_iter, total=expected_len):
            key = (tic_id, aperture, type_)
            try:
                id_ = id_map[key]
                job = SingleMergeJob(
                    tic_id=tic_id,
                    aperture=aperture,
                    lightcurve_type=type_,
                    id=id_,
                )
            except KeyError:
                # New lightcurve!
                params = dict(
                    tic_id=tic_id,
                    aperture_id=aperture,
                    lightcurve_type_id=type_,
                    id=db.session.execute(id_seq),
                )

                id_map[key] = params["id"]
                new_lc_params.append(params)
                job = SingleMergeJob(
                    tic_id=int(params["tic_id"]),
                    aperture=str(params["aperture_id"]),
                    lightcurve_type=str(params["lightcurve_type_id"]),
                    id=int(params["id"]),
                )
            jobs.append(job)

        if len(new_lc_params) > 0:
            click.echo("COPYing {0} new lightcurves...".format(
                len(new_lc_params)   
            ))
            conn = db.session.connection().connection

            mgr = CopyManager(
                conn,
                Lightcurve.__tablename__,
                ['id', 'tic_id', 'aperture_id', 'lightcurve_type_id']
            )

            mgr.threading_copy(
                (
                    r["id"],
                    r["tic_id"],
                    r["aperture_id"],
                    r["lightcurve_type_id"]
                )
                for r in tqdm(new_lc_params)
            )
            conn.commit()
            conn.close()

    # LCDB session is no longer needed, release it
    jobs = pd.DataFrame(jobs, columns=SingleMergeJob._fields)
    jobs.set_index('id', inplace=True)
    click.echo(jobs)

    grouped = jobs.groupby(by=partition_id)

    partition_wise_jobs = []
    click.echo("Grouping lightcurve jobs by partition ID")
    for id_, group in tqdm(grouped):
        partition_wise_jobs.append(
            (
                id_,
                id_ + 1000,
                group,
                new_obs, 
                tic_parameters
            )
        )

    with Pool(n_processes) as pool:
        results = pool.imap(
            partition_copier,
            product(
                [time_corrector], [quality_flags], [orbit_map], partition_wise_jobs
            ),
        )
        for path in results:
            click.echo(
                "Submissing {0}".format(
                    path
                )
            )

@lightcurve.group()
@click.pass_context
@click.argument('blob_path', type=click.Path(dir_okay=False, exists=True))
def blob(ctx, path):
    ctx.obj["blob_path"] = path


@blob.command()
@click.pass_context
def print_observations(ctx):
    with ctx["dbconf"] as db:
        reader = LightcurvePartitionReader(ctx["blob_path"])
        click.echo(
            reader.print_observations(db)
        )


@blob.command()
@click.pass_context
def print_lightpoints(ctx):
    with ctx["dbconf"] as db:
        reader = LightcurvePartitionReader(ctx["blob_path"])
        click.echo(
            reader.print_lightpoints(db)
        )
