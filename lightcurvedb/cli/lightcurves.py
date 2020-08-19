from __future__ import division, print_function

import itertools
import os
import re
from functools import partial
from multiprocessing import Manager, Pool, Process, cpu_count

import click
import pandas as pd

from lightcurvedb.cli.base import lcdbcli
from lightcurvedb.cli.types import CommaList
from lightcurvedb.core.ingestors.temp_table import FileObservation, TIC8Parameters, QualityFlags
from lightcurvedb.core.ingestors.cache import IngestionCache
from lightcurvedb.legacy.timecorrect import StaticTimeCorrector
from lightcurvedb.core.ingestors.lightpoint import LightpointH5Merger, LightpointInserter, MergeJob
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
@click.option('--n-producers', default=44, type=click.IntRange(min=1))
@click.option('--n-consumers', default=4, type=click.IntRange(min=1))
@click.option('--cameras', type=CommaList(int), default='1,2,3,4')
@click.option('--ccds', type=CommaList(int), default='1,2,3,4')
@click.option('--orbit-dir', type=click.Path(exists=True, file_okay=False), default='/pdo/qlp-data')
@click.option('--scratch', type=click.Path(exists=True, file_okay=False), default='/scratch/')
@click.option('--update-type', type=IngestionMode, default=INGESTION_MODES[0])
def ingest_h5(ctx, orbits, n_producers, n_consumers, cameras, ccds, orbit_dir, scratch, update_type):

    cache = IngestionCache()
    click.echo('Connected to ingestion cache, determining filepaths')
    tic_q = cache.session.query(
        FileObservation.tic_id
    ).filter(
        FileObservation.orbit_number.in_(orbits),
        FileObservation.camera.in_(cameras),
        FileObservation.ccd.in_(ccds)
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

    tics = [r for r, in tic_q.distinct().all()]

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
        merge_queue = m.Queue()
        time_corrector = StaticTimeCorrector(db.session)
        click.echo('Preparing {} producers'.format(n_producers))
        producers = []

        for i in range(n_producers):
            producer = LightpointH5Merger(
                merge_queue,
                job_queue,
                time_corrector,
                quality_flags,
                daemon=True
            )
            producers.append(producer)

        click.echo('Preparing {} consumers'.format(n_consumers))
        consumers = [
            LightpointInserter(db._config, job_queue, update_type, daemon=True)
            for _ in range(n_consumers)
        ]

        click.echo('Starting processes...')
        for process in itertools.chain(producers, consumers):
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
                FileObservation.tic_id == tic
            ).order_by(FileObservation.orbit_number.asc())

            if update_type in ('ignore', 'smart'):
                seen_orbits = [r for r, in db.query(Observation.orbit).filter(
                    Observation.tic_id == tic
                ).all()]

                if len(seen_orbits) > 0:
                    file_q = file_q.filter(
                        ~FileObservation.orbit_number.in_(seen_orbits)
                    )

            files = file_q.all()

            if len(files) == 0 :
                # Don't pollute queue,
                continue

            # Perform relatively expensive processes
            cur_id_map = {
                (lc.tic_id, lc.aperture_id, lc.lightcurve_type_id): lc.id
                for lc in db.lightcurves.filter(Lightcurve.tic_id == tic).all()
            }

            job = MergeJob(
                tic_id=tic,
                ra=stellar_parameters.right_ascension,
                dec=stellar_parameters.declination,
                tmag=stellar_parameters.tmag,
                file_observations=files,
                cur_id_map=cur_id_map
            )
            merge_queue.put(job)

    for process in itertools.chain(producers, consumers):
        process.join()

    job_queue.join()
    click.echo(
        click.style('Done', fg='green', bold=True)
    )


@lightcurve.command()
@click.pass_context
@click.argument('tics', type=int, nargs=-1)
@click.option('--orbit-dir', type=click.Path(exists=True, file_okay=False), default='/pdo/qlp-data')
def manual_ingest(ctx, tics, orbit_dir):

    cache = IngestionCache()

    with closed_db as db:
        # Grab orbits
        click.echo('Loading orbits')
        orbits = db.orbits.order_by(Orbit.orbit_number.asc()).all()
        numbers = {o.orbit_number for o in orbits}
        paths = [
            determine_orbit_path(orbit_dir, o, cam, ccd)
            for o, cam, ccd in itertools.product(numbers, [1,2,3,4],[1,2,3,4])
        ]

        tic8 = TIC8Session()
        click.echo('Building observation map')
        cache.load_observations(
            db.observation_df
        )

        click.echo('Parsing files...')
        for path in paths:
            cache.load_dir_to_jobs(
                path,
            )

            click.echo('\tLoaded {}'.format(path))

        cache.session.query(
            IngestionJob
        ).filter(
            ~IngestionJob.tic_id.in_(tics)
        ).delete(synchronize_session=None)

        cache.load_tic8_parameters(tic8)
        job_sqlite.commit()
        tic8.close()

        perform_async_ingestion(
            tics, db
        )

    cache.close()
