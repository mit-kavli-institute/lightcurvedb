from __future__ import division, print_function

import itertools
import os
import re
from functools import partial
from multiprocessing import Manager, Pool, Process, cpu_count
from glob import glob

import click
import pandas as pd

from lightcurvedb.cli.base import lcdbcli
from lightcurvedb.cli.types import CommaList
from lightcurvedb.core.ingestors.temp_table import FileObservation, TIC8Parameters, QualityFlags
from lightcurvedb.core.ingestors.cache import IngestionCache
from lightcurvedb.core.datastructures.data_packers import mass_ingest
from lightcurvedb.legacy.timecorrect import StaticTimeCorrector
from lightcurvedb.core.ingestors.lightpoint import MergeJob, MassIngestor, CopyProcess
from lightcurvedb.models import Lightcurve, Observation, Orbit, QLPProcess
from lightcurvedb.util.iter import chunkify, eq_partitions
from sqlalchemy import BigInteger, Column, Table, and_
from sqlalchemy.orm import sessionmaker


INGESTION_MODES = [
    'smart',
    'ignore',
    'full'
]

IngestionMode = click.Choice(
    INGESTION_MODES
)


def yield_optimized_tics(ordered_tics, all_tics):
    for tic in ordered_tics:
        try:
            all_tics.remove(tic)
            yield tic
        except KeyError:
            continue
    for tic in all_tics:
        yield tic


def ingest_by_tics(ctx, file_observations, tics, cache, n_processes, scratch):

    # Force uniqueness of TICs
    tics = set(map(int, tics))

    quality_flags = pd.read_sql(
        cache.session.query(
            QualityFlags.cadence.label('cadences'),
            QualityFlags.camera,
            QualityFlags.ccd,
            QualityFlags.quality_flag.label('quality_flags')
        ).statement,
        cache.session.bind,
        index_col=['cadences', 'camera', 'ccd']
    )

    click.echo(
        'Will process {} TICs'.format(
            click.style(
                str(len(tics)),
                bold=True
            )
        )
    )

    previously_seen = set()

    with ctx.obj['dbconf'] as db:
        m = Manager()
        job_queue = m.Queue(10000)
        time_corrector = StaticTimeCorrector(db.session)
        workers = []

        click.echo('Grabbing current observatiob table to reduce duplicate work')
        q = db.query(
            Observation.tic_id,
            Orbit.orbit_number
        ).join(Observation.orbit)

        previously_seen = {(tic, orbit) for tic, orbit in q.all()}

        for i in range(n_processes):
            producer = MassIngestor(
                db._config,
                quality_flags,
                time_corrector,
                job_queue,
                scratch,
                daemon=True
            )
            workers.append(producer)

        click.echo('Determining TIC job ordering to optimize PSQL cache hits...')
        tic_q = db\
                .query(Lightcurve.tic_id)\
                .order_by(Lightcurve.id)\
                .distinct()

        optimized_tics = []
        seen = set()
        for tic, in tic_q.all():
            if tic not in seen and tic in tics:
                optimized_tics.append(tic)
                seen.add(tic)

        click.echo(
            'Optimized {} out of {} tics'.format(
                len(optimized_tics), len(tics)
            )
        )

    # Exit DB to remove idle session
    click.echo('Starting processes...')

    for process in workers:
        process.start()

    for tic in yield_optimized_tics(optimized_tics, tics):
        files = []

        df = file_observations[file_observations['tic_id'] == tic]

        for _, file_ in df.iterrows():
            tic, orbit, _, _, _  = file_
            if (file_.tic_id, file_.orbit_number) not in previously_seen:
                files.append((
                    file_.tic_id,
                    file_.orbit_number,
                    file_.camera,
                    file_.ccd,
                    file_.file_path
                ))

        if len(files) == 0:
            # Skip this work
            continue

        stellar_parameters = cache.session.query(
            TIC8Parameters
        ).get(tic)
    
        job = MergeJob(
            tic_id=tic,
            ra=stellar_parameters.right_ascension,
            dec=stellar_parameters.declination,
            tmag=stellar_parameters.tmag,
            file_observations=files
        )
        job_queue.put(job)

    job_queue.join()
    click.echo(
        'Merge-job queue empty'
    )
    for process in workers:
        process.join()
        click.echo(
            'Joined process {}'.format(process)
        )
    
    click.echo(
        click.style('Done', fg='green', bold=True)
    )


@lcdbcli.group()
@click.pass_context
def lightcurve(ctx):
    pass


@lightcurve.command()
@click.pass_context
@click.argument('orbits', type=int, nargs=-1)
@click.option('--n-processes', default=48, type=click.IntRange(min=1))
@click.option('--cameras', type=CommaList(int), default='1,2,3,4')
@click.option('--ccds', type=CommaList(int), default='1,2,3,4')
@click.option('--scratch', type=click.Path(exists=True, file_okay=False), default='/scratch/tmp/lcdb_ingestion')
@click.option('--update-type', type=IngestionMode, default=INGESTION_MODES[0])
def ingest_h5(ctx, orbits, n_processes, cameras, ccds, scratch, update_type):

    cache = IngestionCache()
    click.echo('Connected to ingestion cache, determining filepaths')
    tic_subq = cache.session.query(
        FileObservation.tic_id
    ).filter(
        FileObservation.orbit_number.in_(orbits),
        FileObservation.camera.in_(cameras),
        FileObservation.ccd.in_(ccds)
    ).distinct(FileObservation.tic_id)

    file_obs_q = cache.session.query(
        FileObservation.tic_id,
        FileObservation.orbit_number,
        FileObservation.camera,
        FileObservation.ccd,
        FileObservation.file_path
    ).filter(
        FileObservation.tic_id.in_(tic_subq.subquery())
    )

    df = pd.read_sql(file_obs_q.statement, cache.session.bind)

    ingest_by_tics(ctx, df, df.tic_id, cache, n_processes, scratch)


@lightcurve.command()
@click.pass_context
@click.argument('tics', type=int, nargs=-1)
@click.option('--n-processes', default=48, type=click.IntRange(min=1))
@click.option('--scratch', type=click.Path(exists=True, file_okay=False), default='/scratch/tmp/lcdb_ingestion')
def manual_ingest(ctx, tics, n_processes, scratch):
    cache = IngestionCache()
    click.echo('Connected to ingestion cache, determining filepaths')

    file_obs_q = cache.session.query(
        FileObservation.tic_id,
        FileObservation.orbit_number,
        FileObservation.camera,
        FileObservation.ccd,
        FileObservation.file_path
    )
    file_observations = pd.read_sql(
        file_obs_q.statement,
        cache.session.bind,
        index_col=['tic_id']
    )

    ingest_by_tics(ctx, file_observations, tics, cache, n_processes, scratch)



@lightcurve.command()
@click.pass_context
@click.argument('tic_file', type=click.Path(exists=True, dir_okay=False))
@click.option('--n-processes', default=48, type=click.IntRange(min=1))
@click.option('--scratch', type=click.Path(exists=True, file_okay=False), default='/scratch/tmp/lcdb_ingestion')
def tic_list(ctx, tic_file, n_processes, scratch):
    tics = {int(tic.strip()) for tic in open(tic_file, 'rt').readlines()}
    cache = IngestionCache()
    click.echo('Connected to ingestion cache, determining filepaths')

    quality_flags = pd.read_sql(
        cache.session.query(
            QualityFlags.cadence.label('cadences'),
            QualityFlags.camera,
            QualityFlags.ccd,
            QualityFlags.quality_flag.label('quality_flags')
        ).statement,
        cache.session.bind,
        index_col=['cadences', 'camera', 'ccd']
    )

    file_obs_q = cache.session.query(
        FileObservation.tic_id,
        FileObservation.orbit_number,
        FileObservation.camera,
        FileObservation.ccd,
        FileObservation.file_path
    )
    file_observations = pd.read_sql(
        file_obs_q.statement,
        cache.session.bind,
        index_col=['tic_id']
    )

    ingest_by_tics(ctx, file_observations, tics, cache, n_processes, scratch)


@lightcurve.command()
@click.pass_context
@click.argument('tic', type=int)
def print(ctx, tic):
    """
    Prints a tabular view of the lightcurve to stdout
    """
    with closed_db as db:
        lcs = db.lightcurves.filter(Lightcurve.tic_id == tic).all()
