import click
import os
import itertools
import sys
import numpy as np
import datetime
import io
import logging
from datetime import datetime
from random import sample
from functools import partial
from sqlalchemy import Sequence, Column, BigInteger, Integer
from sqlalchemy.sql import func, text, insert, select, func
from sqlalchemy.sql.expression import bindparam
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION
from sqlalchemy.exc import InvalidRequestError
from sqlalchemy.orm import noload
from collections import OrderedDict, defaultdict
from multiprocessing import Pool, cpu_count, SimpleQueue, Manager
from lightcurvedb.core.base_model import QLPModel, QLPDataProduct
from lightcurvedb.models import Aperture, Orbit, LightcurveType, Lightcurve
from lightcurvedb.core.ingestors.lightcurve_ingestors import h5_to_matrices
from lightcurvedb.core.ingestors.lightpoint import get_raw_h5, get_cadence_info
from lightcurvedb.util.logging import make_logger
from lightcurvedb.util.iter import partition, chunkify, partition_by
from lightcurvedb.util.merge import matrix_merge
from lightcurvedb.util.lightpoint_util import map_existing_lightcurves, create_lightpoint_tmp_table
from lightcurvedb.reportings.lightcurve_ingest_report import IngestReport
from lightcurvedb.core.connection import db_from_config
from glob import glob
from h5py import File as H5File
from .base import lcdbcli
from .utils import find_h5
from .types import CommaList
 
logging.getLogger('sqlalchemy.dialects.postgresql').setLevel(logging.INFO)
logger = make_logger('H5 Ingestor')
@lcdbcli.group()
@click.pass_context
def lightcurve(ctx):
    pass


def determine_orbit_path(orbit_dir, orbit, cam, ccd):
    orbit_name = 'orbit-{}'.format(orbit)
    cam_name = 'cam{}'.format(cam)
    ccd_name = 'ccd{}'.format(ccd)
    return os.path.join(orbit_dir, orbit_name, 'ffi', cam_name, ccd_name, 'LC')

def extr_tic(filepath):
    return int(os.path.basename(filepath).split('.')[0])

def make_lc_key(lightcurve):
    return lightcurve.tic_id, lightcurve.lightcurve_type_id, lightcurve.aperture_id, lightcurve.cadence_type


def lc_to_dict(lc):
    return {
        '_id': lc.id, 'cadences': lc.cadences, 'barycentric_julian_date': lc.bjd, 'values': lc.values,
        'errors': lc.errors, 'x_centroids': lc.x_centroids, 'y_centroids': lc.y_centroids,
        'quality_flags': lc.quality_flags
    }


def map_lightcurves(session, tics):
    q = session.query(Lightcurve).filter(
        Lightcurve.tic_id.in_(tics)
    ).execution_options(stream_results=True, max_row_buffer=len(tics)*10)
    return {
        make_lc_key(lc): lc for lc in q.all()
    }

def ingest_files(config, cadence_type, lc_type_map, aperture_map, job):
    pid = os.getpid()
    new_lcs = {}
    tics = job['tics']
    files_by_tic = job['files_by_tic']
    worker_name = f'Worker-{pid}'
    qlp_id_seq = Sequence('qlpdataproducts_pk_table')
    with db_from_config(config, server_side_cursors=True) as db:
        logger.info(f'{worker_name}: connected to db, preparing query for {len(tics)} tics')
        lightcurve_map = map_lightcurves(db.session, tics)
        logger.info(f'{worker_name}: created lightcurve_map with {len(lightcurve_map)} entries')
        logger.info(f'{worker_name}: processing files...')
        for f in itertools.chain(*files_by_tic):
            h5 = get_raw_h5(f)
            for raw_lc in h5:
                type_id = lc_type_map[raw_lc['lc_type']]
                aperture_id = aperture_map[raw_lc['aperture']]
                tic = raw_lc['tic']
                data = raw_lc['data']
                length = data.shape[1]
                key = (tic, type_id, aperture_id, cadence_type)

                if key in lightcurve_map:
                    # Merge lightcurve
                    prev_data = lightcurve_map[key]
                    #merge_lc_dict(prev_data, data)
                else:
                    # Brand new lightcurve
                    if key in new_lcs:
                        merge_lc(new_lcs[key], data)
                    else:
                        new_lc = {
                            'id': db.session.execute(qlp_id_seq),
                            'tic_id': tic,
                            'cadence_type': cadence_type,
                            'aperture_id': aperture_id,
                            'lightcurve_type_id': type_id,
                            '_cadences': data[0],
                            '_bjd': data[1],
                            '_values': data[2],
                            '_errors': data[3],
                            '_x_centroids': data[4],
                            '_y_centroids': data[5],
                            '_quality_flags': data[6],
                        }
                        new_lcs[key] = new_lc

        logger.info(f'{worker_name}: inserting lightcurves and qlpdataproducts')
        for chunk in chunkify(new_lcs.values(), 10000):
            inheritance_mappings = [
                {
                    'id': lc['id'],
                    'created_on': datetime.now(),
                    'product_type': Lightcurve.__tablename__
                }
                for lc in chunk
            ]
            for kwargs in chunk:
                for kwarg in ['cadences', 'barycentric_julian_date', 'values', 'errors', 'x_centroids', 'y_centroids', 'quality_flags']:
                    kwarg[kwarg] = kwargs[kwarg].tolist()

            db.session.bulk_insert_mappings(QLPDataProduct, inheritance_mappings)
            db.session.bulk_insert_mappings(Lightcurve, chunk)
            logger.info(f'{worker_name}: Inserted {len(inhertiance_mappings)} QLPDataProducts')
            logger.info(f'{worker_name}: Inserted {len(chunk)} new lightcurves')
        db.session.commit()
        logger.info(f'{worker_name}: done')
    return {'n_inserted': len(new_lcs), 'n_merged': len(lightcurve_map)}


def merge_lc(lc, new_data):
    old_data = lc.to_np
    merged = matrix_merged(old_data, new_data)
    lc.cadences = merged[0]
    lc.bjd = merged[1]
    lc.values = merged[2]
    lc.errors = merged[3]
    lc.x_centroids = merged[4]
    lc.y_centroids = merged[5]
    lc.quality_flags = merged[6]

def merge_lc_dict(lc_dict, new_data, new=False):
    LC_KWARGS = ['_cadences', '_bjd', '_values', '_errors', '_x_centroids', '_y_centroids', '_quality_flags']
    compiled = np.array([lc_dict[x] for x in LC_KWARGS])
    merged = matrix_merge(compiled, new_data)
    for row, kw in enumerate(LC_KWARGS):
        lc_dict[kw] = merged[row]

@lightcurve.command()
@click.pass_context
@click.argument('orbits', type=int, nargs=-1)
@click.option('--n-process', type=int, default=-1, help='Number of cores. <= 0 will use all cores available')
@click.option('--cameras', type=CommaList(int), default='1,2,3,4')
@click.option('--ccds', type=CommaList(int), default='1,2,3,4')
@click.option('--orbit-dir', type=click.Path(exists=True, file_okay=False), default='/pdo/qlp-data')
@click.option('--cadence_type', type=int, default=30)
@click.option('--n-lp-insert', type=int, default=10**5)
def ingest_h5(ctx, orbits, n_process, cameras, ccds, orbit_dir, cadence_type, n_lp_insert):
    with ctx.obj['dbconf'] as db:
        orbits = db.orbits.filter(Orbit.orbit_number.in_(orbits)).all()
        orbit_numbers = [o.orbit_number for o in orbits]
        apertures = db.apertures.all()
        lc_types = db.lightcurve_types.filter(
            LightcurveType.name.in_(('KSPMagnitude', 'RawMagnitude'))
        ).all()

        aperture_map = {
            a.name: a.id for a in apertures
        }
        lc_type_map = {
            lt.name: lt.id for lt in lc_types
        }

        click.echo(
            'Ingesting {} orbits with {} apertures'.format(len(orbits), len(apertures))
        )

        if n_process <= 0:
            n_process = cpu_count()

        click.echo(
            'Utilizing {} cores'.format(click.style(str(n_process), bold=True))
        )

        path_iterators = []
        for cam, ccd, orbit in itertools.product(cameras, ccds, orbit_numbers):
            lc_path = determine_orbit_path(orbit_dir, orbit, cam, ccd)
            path_iterators.append(find_h5(lc_path))

        files_partitioned_by_tic = partition_by(
            itertools.chain(*path_iterators),
            n_process,
            key=extr_tic
        )

        all_tics = set()

        jobs = []

        for partition in files_partitioned_by_tic:
            tics = {grp[0] for grp in partition}
            files = [list(grp[1]) for grp in partition]
            total_files = sum(len(l) for l in files)
            click.echo(f'\tGenerating partition with {len(tics)} tics and {total_files}')
            jobs.append(
                {
                    'tics': tics,
                    'files_by_tic': files
                }
            )
            all_tics |= tics

        click.echo(f'Made {len(jobs)} partitions')
        click.echo(f'Pool will process {len(tics)} tics')

        if not ctx.obj['dryrun']:
            prompt = click.style('Does this information look ok?', bold=True)
            click.confirm(prompt, abort=True)
            click.echo('\tBeginning interpretation of new lightcurves')
        else:
            return

        func = partial(
            ingest_files,
            db._config,
            cadence_type,
            lc_type_map,
            aperture_map,
        )
        try:
            import_start_time = datetime.now()
            with Pool(n_process) as p:
                click.echo('\tPreparing worker pool')
                results = p.imap_unordered(
                    func,
                    jobs
                )
                click.echo('\tExecuting pool...')
                n_inserted = 0
                n_merged = 0
                for job_result in results:
                    n_inserted += job_result['n_inserted']
                    n_merged += job_result['n_merged']
                click.echo(
                    'Ingestion merged {} and inserted {} new lightcurves'.format(
                        click.style(str(n_merged), bold=True, fg='yellow'),
                        click.style(str(n_inserted), bold=True, fg='green')
                    )
                )
        except:
            db.session.rollback()
            raise

@lightcurve.command()
@click.pass_context
@click.argument('orbits', type=int, nargs=-1)
@click.option('--cameras', type=CommaList(int), default='1,2,3,4')
@click.option('--ccds', type=CommaList(int), default='1,2,3,4')
def ingest_quality_flags(ctx, orbits, cameras, ccds):
    with ctx.obj['dbconf'] as db:
        orbits = orbit.orbits.filter(Orbit.orbit_number.in_(orbits)).all()

        for orbit, camera, ccd in itertools.product(orbits, cameras, ccds):
            run_path = os.path.join(
                ctx['qlp_data'],
                'orbit-{}'.format(orbit.orbit_number),
                'ffi', 'run')
            qflagfile = f'cam{camera}ccd{ccd}_qflag.txt'
            path = os.path.join(run_path, qflagfile)
            pass
