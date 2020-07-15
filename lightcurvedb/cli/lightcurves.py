import click
import os
import itertools
import sys
import numpy as np
import re
import pandas as pd
from tabulate import tabulate
from functools import partial
from sqlalchemy import func, and_, Table, Column, BigInteger
from sqlalchemy.orm import sessionmaker
from collections import defaultdict
from multiprocessing import Manager, Queue, Pool, cpu_count
from lightcurvedb.models import Aperture, Orbit, LightcurveType, Lightcurve, Observation
from lightcurvedb.core.ingestors.lightcurve_ingestors import h5_to_kwargs, lc_dict_to_df, parallel_h5_merge
from lightcurvedb.managers.lightcurve_query import LightcurveManager
from lightcurvedb.util.iter import partition, chunkify, partition_by
from lightcurvedb.util.logger import lcdb_logger as logger
from lightcurvedb.core.connection import db_from_config
from lightcurvedb.core.tic8 import TIC8_ENGINE, TIC_Entries, TIC8_Base
from lightcurvedb.core.ingestors.temp_table import TempSession, IngestionJob, TempObservation, TIC8Parameters, LightcurveIDMapper
from lightcurvedb.core.base_model import QLPModel
from lightcurvedb import db as closed_db
from glob import glob
from h5py import File as H5File
from .base import lcdbcli
from .utils import find_h5, extr_tic, group_h5
from .types import CommaList


TIC8Session = sessionmaker(autoflush=True)
TIC8Session.configure(bind=TIC8_ENGINE)


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
        fullpath = os.path.join(path, f'{tic}.h5')
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


@lightcurve.command()
@click.pass_context
@click.argument('orbits', type=int, nargs=-1)
@click.option('--n-process', type=int, default=-1, help='Number of cores. <= 0 will use all cores available')
@click.option('--n-tics', type=int, default=1000, help='Number of tics to be given per worker')
@click.option('--cameras', type=CommaList(int), default='1,2,3,4')
@click.option('--ccds', type=CommaList(int), default='1,2,3,4')
@click.option('--orbit-dir', type=click.Path(exists=True, file_okay=False), default='/pdo/qlp-data')
@click.option('--scratch', type=click.Path(exists=True, file_okay=False), default='/scratch/')
@click.option('--smart-update/--force-full-update', default=True)
def ingest_h5(ctx, orbits, n_process, n_tics, cameras, ccds, orbit_dir, scratch, smart_update):
    current_tmp_id = -1

    # Create a sqlite session to hold job contexts across processes
    job_sqlite = TempSession()

    with closed_db as db:
        orbits = db.orbits.filter(Orbit.orbit_number.in_(orbits)).all()
        orbit_numbers = sorted([o.orbit_number for o in orbits])
        orbit_map = {
            orbit.orbit_number: orbit.id for orbit in orbits
        }

        if n_process <= 0:
            n_process = cpu_count()

        click.echo(
            'Utilizing {} cores'.format(click.style(str(n_process), bold=True))
        )

        with Pool(n_process) as p:
            m = Manager()
            q = m.Queue()
            func = partial(parallel_search_h5, q)

            p.starmap(
                func,
                itertools.product(
                    [orbit_dir],
                    orbit_numbers,
                    cameras,
                    ccds
                )
            )
            # Queue now contains list of dicts
            accumulator = []
            while not q.empty():
                component = q.get()
                accumulator.append(component)
        
        file_df = pd.DataFrame(accumulator)
        observation_df = observation_map(db)

        click.echo('Determining needed ingestion jobs')
        ref_tics = file_df['tic_id'].unique()

        job_sqlite.bulk_insert_mappings(IngestionJob, file_df.to_dict('records'))
        job_sqlite.bulk_insert_mappings(TempObservation, observation_df.to_dict('records'))

        job_sqlite.commit()

        if smart_update:
            # Only process new observations, delete any ingestion job that already
            # has a defined observation
            click.echo(f'Determining merge solutions')
            bad_ids = job_sqlite.query(
                IngestionJob.id
            ).join(
                TempObservation,
                and_(
                    IngestionJob.tic_id == TempObservation.tic_id,
                    IngestionJob.orbit_number == TempObservation.orbit_number
                )
            )
            n_bad = bad_ids.count()

            job_sqlite.query(IngestionJob).filter(IngestionJob.id.in_(bad_ids.subquery())).delete(synchronize_session=False)

            job_sqlite.commit()
            click.echo(
                'Removed {} jobs as these files have already been ingested'.format(
                    click.style(str(n_bad), fg='green', bold=True)
                )
            )
            click.echo(
                'Will process {} h5 files!'.format(
                    click.style(str(job_sqlite.query(IngestionJob).count()), fg='yellow', bold=True)
                )
            )
        else:
            # Look at all the files, process everything
            pass

        n_jobs = job_sqlite.query(IngestionJob.id).count()
        tics = {r for r, in job_sqlite.query(IngestionJob.tic_id).distinct().all()}

        # Create TIC8 Session and load in necessary stellar parameters
        tic8 = TIC8Session()

        # Create temporary tables to allow for potentially enourmous query sizes
        tic8_tmp_table = Table(
            'mass_tic_ref',
            TIC8_Base.metadata,
            Column('tic_id', BigInteger, primary_key=True),
            prefixes=['TEMPORARY']
        )
        lc_kwarg_tmp_table = Table(
            'mass_tic_kwarg_ref',
            QLPModel.metadata,
            Column('tic_id', BigInteger, primary_key=True),
            prefixes=['TEMPORARY']
        )

        #  Chunkify to reduce querysize
        click.echo('Grabbing needed info from TIC8')
        tic8_tmp_table.create(bind=tic8.bind)
        tic8.commit()
        tic8.execute(
            tic8_tmp_table.insert(),
            [{'tic_id': tic} for tic in tics]
        )
        click.echo('\tPopulated TMP TIC8 Table')
        tic8.commit()

        tic8_params = pd.read_sql(
            tic8.query(
                TIC_Entries.c.id.label('tic_id'),
                TIC_Entries.c.ra.label('right_ascension'),
                TIC_Entries.c.dec.label('declination'),
                TIC_Entries.c.tmag.label('tmag'),
                TIC_Entries.c.e_tmag.label('tmag_error')
            ).join(
                tic8_tmp_table, TIC_Entries.c.id == tic8_tmp_table.c.tic_id
            ).statement,
            tic8.bind
        )
        job_sqlite.bulk_insert_mappings(
            TIC8Parameters,
            tic8_params.to_dict('records')
        )

        click.echo('Loading defined lightcurve identifiers')
        lc_kwarg_tmp_table.create(bind=db.session.bind)
        db.session.commit()

        db.session.execute(
            lc_kwarg_tmp_table.insert(),
            [{'tic_id': tic} for tic in tics]
        )
        db.commit()

        # Load in ID Map
        q = db.query(
            Lightcurve.id,
            Lightcurve.tic_id,
            Lightcurve.aperture_id,
            Lightcurve.lightcurve_type_id
        ).join(
            lc_kwarg_tmp_table,
            Lightcurve.tic_id == lc_kwarg_tmp_table.c.tic_id
        )

        lc_kwargs = [
            {
                'id': row[0],
                'tic_id': row[1],
                'aperture': row[2],
                'lightcurve_type': row[3]
            }
            for row in q.all()
        ]
        job_sqlite.bulk_insert_mappings(
            LightcurveIDMapper,
            lc_kwargs
        )
        
        click.echo('\tLoaded LC Kwargs')

        job_sqlite.commit()
    click.echo(f'Will process {len(tics)} TICs')

    jobs = [chunk for chunk in chunkify(tics, n_tics)]

    with Pool(n_process) as p:
        func = partial(parallel_h5_merge, ctx.obj['dbconf']._config)
        results = p.imap_unordered(func, jobs)
        for nth, result in enumerate(results):
            click.echo(f'Worker successfully processed {result} tics. Job ({nth+1}/{len(jobs)})')

    job_sqlite.close()


@lightcurve.command()
@click.pass_context
@click.argument('ticlist', type=click.Path(exists=True, dir_okay=False))
@click.option('--orbit-dir', type=click.Path(exists=True, file_okay=False), default='/pdo/qlp-data')
def manual_ingest(ctx, ticlist, orbit_dir):

    tics = [int(ticstr.split('.')[0]) for ticstr in open(ticlist, 'rt').readlines()]

    job_sqlite = TempSession()

    with closed_db as db:
        # Grab orbits
        orbits = db.orbits.all()
        numbers = {o.orbit_number for o in orbits}
        paths = [determine_orbit_path(orbit_dir, o, cam, ccd) for o, cam, ccd in itertools.product(numbers, [1,2,3,4],[1,2,3,4])]
        raw_lcs = []
        for tic in tics:
            raw_lcs += list(filter_for_tic(paths, tic))

        job_sqlite.bulk_insert_mappings(IngestionJob, raw_lcs)
        job_sqlite.commit()

        tic8 = TIC8Session()

        params = pd.read_sql(
            tic8.query(
                TIC_Entries.c.id.label('tic_id'),
                TIC_Entries.c.ra.label('right_ascension'),
                TIC_Entries.c.dec.label('declination'),
                TIC_Entries.c.tmag.label('tmag'),
                TIC_Entries.c.e_tmag.label('tmag_error')
            ).filter(TIC_Entries.c.id.in_(tics)).statement,
                tic8.bind
        )

        job_sqlite.bulk_insert_mappings(
            TIC8Parameters,
            params.to_dict('records')
        )

        q = db.query(
                Lightcurve.id,
                Lightcurve.tic_id,
                Lightcurve.aperture_id,
                Lightcurve.lightcurve_type_id
            ).filter(Lightcurve.tic_id.in_(tics))
        lc_kwargs = [dict(zip(row.keys(), row)) for row in q.all()]

        job_sqlite.bulk_insert_mappings(
            LightcurveIDMapper,
            lc_kwargs
        )

        job_sqlite.commit()

        parallel_h5_merge(ctx.obj['dbconf']._config, tics)
    job_sqlite.close()
