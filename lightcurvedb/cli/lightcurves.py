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
from lightcurvedb.core.ingestors.lightpoint import MergeJob, MassIngestor
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


@lcdbcli.group()
@click.pass_context
def lightcurve(ctx):
    pass


def determine_orbit_path(orbit_dir, orbit, cam, ccd):
    orbit_name = 'orbit-{}'.format(orbit)
    cam_name = 'cam{}'.format(cam)
    ccd_name = 'ccd{}'.format(ccd)
    return os.path.join(orbit_dir, orbit_name, 'ffi', cam_name, ccd_name, 'LC')

def determine_h5_path_components(h5path):
    search = r'orbit-(?P<orbit_number>[1-9][0-9]*)/ffi/cam(?P<camera>[1-4])/ccd(?P<ccd>[1-4])/LC/(?P<tic_id>[1-9][0-9]*)\.h5$'
    match = re.search(search, h5path)
    if match:
        return match.groupdict()
    return None

def observation_map(session):
    q = session.query(
        Observation.tic_id,
        Observation.camera,
        Observation.ccd,
        Orbit.orbit_number
    ).join(Observation.orbit)

    observation_df = pd.read_sql(
        q.statement,
        session.session.bind,
    )
    return observation_df

def files_to_dict(orbits, cameras, ccds, orbit_dir):
    for orbit, camera, ccd in itertools.product(orbits, cameras, ccds):
        path = determine_orbit_path(orbit_dir, orbit, camera, ccd)
        for h5 in find_h5(path):
            components = determine_h5_path_components(h5)
            if components:
                yield dict(
                    file_path=h5,
                    **components
                )


def files_to_df(orbits, cameras, ccds, orbit_dir):
    df = pd.DataFrame(
        list(files_to_dict(orbits, cameras, ccds, orbit_dir))
    )
    return df


def filter_for_tic(base_file_paths, tic):
    for path in base_file_paths:
        fullpath = os.path.join(path, '{}.h5'.format(tic))
        if os.path.exists(fullpath):
            components = determine_h5_path_components(fullpath)
            if components:
                yield dict(
                    file_path=fullpath,
                    **components
                )


def parallel_search_h5(queue, orbit_path, orbit, camera, ccd):
    path = determine_orbit_path(orbit_path, orbit, camera, ccd)
    for h5 in find_h5(path):
        components = determine_h5_path_components(h5)
        if components:
            queue.put(
                dict(
                    file_path=h5,
                    **components
                )
            )


def prepare_ingestions(cache, smart_ingest):
    click.echo('Determining needed ingestion jobs')
    if smart_ingest:
        n_bad, remaining = cache.remove_duplicate_jobs()
        click.echo(
            'Removed {} jobs as these files have already been ingested'.format(
                click.style(str(n_bad), fg='green', bold=True)
            )
        )
        click.echo(
            'Will process {} h5 files!'.format(
                click.style(str(remaining), fg='yellow', bold=True)
            )
        )
        cache.commit()


def get_from_tic8(tics):
    tic8 = TIC8Session()
    # Load temporary table
    tic8_params = pd.read_sql(
        tic8.query(
            TIC_Entries.c.id.label('tic_id'),
            TIC_Entries.c.ra.label('right_ascension'),
            TIC_Entries.c.dec.label('declination'),
            TIC_Entries.c.tmag.label('tmag'),
        ).filter(
            TIC_Entries.c.id.in_(tics)   
        ).statement,
        tic8.bind
    )
    tic8.close()
    return tic8_params

def determine_tic_info(orbits, cameras, tics, scratch_dir):
    # First attempt to read TIC8 Caches. Start by finding all
    # relevant cache files
    return get_from_tic8(tics)


def preload_qflags(t_db, orbits):
    physical_labels = [1,2,3,4]

    for cam, ccd, orbit in itertools.product(physical_labels, physical_labels, orbits):
        path = os.path.join(
            orbit.get_qlp_run_directory(),
            'cam{}ccd{}_qflag.txt'.format(cam, ccd)
        )
        load_qflag_file(t_db, path)
    t_db.commit()

def perform_async_ingestion(tics, db):
    m = Manager()
    job_queue = m.Queue()
    lightcurve_queue = m.Queue(maxsize=10000)
    time_corrector = StaticTimeCorrector(
        db.session
    )
    engine_kwargs = dict(
        executemany_mode='values',
        executemany_values_page_size=10000,
        executemany_batch_page_size=500
    )

    producers = [
        Process(
            target=async_h5_merge,
            args=(
                ctx.obj['dbconf']._config,
                job_queue,
                lightcurve_queue,
                time_corrector
            ),
            daemon=True
        )

        for _ in range(n_producers)
    ]
    consumers = [
        DBLoader(
            ctx.obj['dbconf']._config,
            lightcurve_queue,
            daemon=True
        )
        for _ in range(n_consumers)
    ]

    for p in itertools.chain(producers, consumers):
        p.start()
    for i, tic in enumerate(tics):
        job_queue.put((
            tic
        ))
    for p in itertools.chain(producers, consumers):
        p.join()


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
    file_obs_q = cache.session.query(
        FileObservation.tic_id,
        FileObservation.orbit_number,
        FileObservation.camera,
        FileObservation.ccd,
        FileObservation.file_path
    ).filter(
        FileObservation.orbit_number.in_(orbits),
        FileObservation.camera.in_(cameras),
        FileObservation.ccd.in_(ccds)
    )

    file_observations = pd.read_sql(
        file_obs_q.statement,
        cache.session.bind,
    )

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

    tics = file_observations.tic_id.unique()

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
        job_queue = m.Queue(maxsize=10000)
        time_corrector = StaticTimeCorrector(db.session)
        click.echo('Preparing {} threads'.format(n_processes))
        workers = []

        click.echo(
            'Looking at observation table to reduce duplicate work'
        )
        q = db.query(
            Observation.tic_id,
            Orbit.orbit_number
        ).join(Observation.orbit).filter(
            Observation.camera.in_(cameras),
            Observation.ccd.in_(ccds),
            Orbit.orbit_number.in_(orbits)
        )

        for tic_id, orbit_number in q.all():
            previously_seen.add((tic_id, orbit_number))

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

    click.echo('Starting processes...')
    for process in workers:
        process.start()

    for tic, df in file_observations.groupby('tic_id'):
        files = []
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

    for process in workers:
        process.join()

    job_queue.join()
    click.echo(
        click.style('Done', fg='green', bold=True)
    )


@lightcurve.command()
@click.pass_context
@click.argument('tics', type=int, nargs=-1)
@click.option('--n-processes', default=48, type=click.IntRange(min=1))
@click.option('--scratch', type=click.Path(exists=True, file_okay=False), default='/scratch/tmp/lcdb_ingestion')
def manual_ingest(ctx, tics, n_processes, scratch):
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


    click.echo(
        'Will process {} TICs'.format(
            click.style(
                str(len(tics)),
                bold=True
            )
        )
    )

    with ctx.obj['dbconf'] as db:
        m = Manager()
        job_queue = m.Queue(maxsize=10000)
        time_corrector = StaticTimeCorrector(db.session)
        click.echo('Preparing {} threads'.format(n_processes))
        workers = []

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


    click.echo('Starting processes...')
    for process in workers:
        process.start()

    for tic in tics:
        stellar_parameters = cache.session.query(
            TIC8Parameters
        ).get(tic)

        file_q = cache.session.query(
            FileObservation.tic_id,
            FileObservation.orbit_number,
            FileObservation.camera,
            FileObservation.ccd,
            FileObservation.file_path
        ).filter(
            FileObservation.tic_id == tic,
            FileObservation.orbit_number.in_(orbits)
        ).order_by(FileObservation.orbit_number.asc())

        files = file_q.all()


        job = MergeJob(
            tic_id=tic,
            ra=stellar_parameters.right_ascension,
            dec=stellar_parameters.declination,
            tmag=stellar_parameters.tmag,
            file_observations=files,
        )
        job_queue.put(job)

    for process in workers:
        process.join()

    job_queue.join()
    click.echo(
        click.style('Done', fg='green', bold=True)
    )


@lightcurve.command()
@click.pass_context
@click.option('--scratch', type=click.Path(file_okay=False, exists=True), default='/scratch/tmp/')
@click.option('--cache-name', type=str, default='lcdb_ingestion')
def recover_blobs(ctx, scratch, cache_name):
    path = os.path.join(
        scratch, cache_name, '*.blob'
    )
    files = glob(path)
    click.echo(
        'Found {} orphan blobs'.format(
            click.style(
                str(len(files)),
                bold=True,
                fg='green'
            )
        )
    )
    with ctx.obj['dbconf'] as db:
        click.echo(
            'Loading defined Lightcurve ids...'
        )
        ids = {r for r, in db.query(Lightcurve.id)}
        cursor = db.session.connection().connection.cursor()
        click.echo(
            'Preprocessing files, ensuring data validity...'
        )
        total_ingested = 0
        for filepath in files:
            df = pd.read_csv(filepath, header=None, names=[
                'lightcurve_id', 'cadence', 'barycentric_julian_date', 'data',
                'error', 'x_centroid', 'y_centroid', 'quality_flag'
            ])
            lightcurve_ids = set(df.lightcurve_id.unique())

            incongruent_ids = lightcurve_ids - ids
            if len(incongruent_ids) > 0:
                click.echo(
                    'Blob is missing {} IDs! Cannot ingest'.format(len(incongruent_ids))
                )
            else:
                mass_ingest(cursor, open(filepath, 'rt'), 'lightpoints', sep=',')
                db.commit()
                total_ingested += len(df)
                click.echo(
                    '\tInserted {} lightpoints'.format(
                        click.style(str(len(df)), bold=True, fg='green')
                    )
                )
                os.remove(filepath)

        click.echo(
            'Done! Sent {} points to the database.'.format(
                click.style(
                    str(total_ingested),
                    bold=True,
                    fg='green'
                )
            )
        )

@lightcurve.command()
@click.pass_context
@click.argument('tic', type=int)
def print(ctx, tic):
    """
    Prints a tabular view of the lightcurve to stdout
    """
    with closed_db as db:
        lcs = db.lightcurves.filter(Lightcurve.tic_id == tic).all()
