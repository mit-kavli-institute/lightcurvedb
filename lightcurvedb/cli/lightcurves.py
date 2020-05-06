import click
import os
import itertools
import sys
import numpy as np
import datetime
import io
import logging
import re
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
from lightcurvedb.util.iter import partition, chunkify, partition_by
from lightcurvedb.util.merge import matrix_merge
from lightcurvedb.reportings.lightcurve_ingest_report import IngestReport
from lightcurvedb.core.connection import db_from_config
from glob import glob
from h5py import File as H5File
from .base import lcdbcli
from .utils import find_h5
from .types import CommaList
 
logger = logging.getLogger(__name__)


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
    search = r'orbit-(?P<orbit>[1-9][0-9]*)/ffi/cam(?P<camera>[1-4])/ccd(?P<ccd>[1-4])/LC/(?P<tic>[1-9][0-9]*)\.h5$'
    match = re.match(search, h5path)
    if match:
        return match.groupdict()
    return None

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
            make_lc_key(lc): {'__data': lc.to_df, '_id': lc.id} for lc in q.all()
    }

def ingest_files(config, cadence_type, lc_type_map, aperture_map, job):
    pid = os.getpid()
    tics = job['tics']
    files_by_tic = job['files_by_tic']
    worker_name = f'Worker-{pid}'
    qlp_id_seq = Sequence('qlpdataproducts_pk_table')
    logger.info(f'Worker-{pid} initialized')
    quality_flag_map = {}

    n_inserted = 0
    n_updated = 0

    with db_from_config(config, server_side_cursors=True) as db:
        logger.info(f'{worker_name}: connected to db')
        logger.info(f'{worker_name}: processing files...')
        for chunk_i, chunk in enumerate(chunkify(files_by_tic, 1000)):
            tics = {extr_tic(f) for f in itertools.chain(*chunk)}
            lightcurve_map = map_lightcurves(db.session, tics)
            logger.info(f'Worker-{pid}: operating on chunk {chunk_i + 1} with length {len(chunk)}')
            logger.info(f'{worker_name}: created lightcurve_map with {len(lightcurve_map)} entries')
            new_lcs = {}
            for f in itertools.chain(*chunk):
                file_context = determine_h5_path_components(f)
                if file_context is None:
                    continue
                orbit = file_context['orbit']
                camera = file_context['camera']
                ccd = file_context['ccd']
                tic = file_context['tic']

                h5 = get_raw_h5(f)
                qflag_key = (
                    orbit, camera, ccd
                )

                try:
                    quality_flags = quality_flag_map[(orbit, camera, ccd)]
                except KeyError:
                    quality_flags = pd.DataFrame(
                        os.path.join(
                            '/',
                            'pdo',
                            'qlp-data',
                            f'orbit-{orbit}',
                            'ffi',
                            'run',
                            f'cam{cam}ccd{ccd}_qflag.txt'
                        ),
                        sep=' ', header=0, names=['cadences', 'quality_flags'],
                        index_col='cadences', dtype=int
                    )
                    quality_flag_map[(orbit, camera, ccd)] = quality_flags

                for raw_lc in h5:
                    type_id = lc_type_map[raw_lc['lc_type']]
                    aperture_id = aperture_map[raw_lc['aperture']]
                    tic = raw_lc['tic']
                    data = raw_lc['data']
                    length = data.shape[1]
                    key = (tic, type_id, aperture_id, cadence_type)

                    new_lc_df = pd.DataFrame({
                        'cadences': data[0],
                        'bjd': data[1],
                        'values': data[2],
                        'errors': data[3],
                        'x_centroids': data[4],
                        'y_centroids': data[5],
                        'quality_flags': data[6]
                    })

                    new_lc_df['quality_flags'] = quality_flags[new_lc_df.index]['quality_flags']
                    if key in lightcurve_map:
                        # Merge lightcurve
                        merged = pd.concat(lightcurve_map[key]['__data'], new_lc_df)
                        merged[~merged.index.duplicated(keep='last')]
                        lightcurve_map[key]['__data'] = merged
                    else:
                        # Brand new lightcurve
                        if key in new_lcs:
                            merged = pd.concat(new_lcs[key]['__data'], new_lc_df)
                            merged = merged[~merged.index.duplicated(keep='last')]
                            merged.sort_index(inplace=True)
                            new_lcs['key']['__data'] = merged
                        else:
                            new_lc = {
                                'id': db.session.execute(qlp_id_seq),
                                'tic_id': tic,
                                'cadence_type': cadence_type,
                                'aperture_id': aperture_id,
                                'lightcurve_type_id': type_id,
                                '__data': new_lc_df
                            }
                            new_lcs[key] = new_lc

            logger.info(f'{worker_name}: inserting lightcurves and qlpdataproducts')
            for new_lc_chunk in chunkify(new_lcs.values(), 10000):
                inheritance_mappings = [
                    {
                        'id': lc['id'],
                        'created_on': datetime.now(),
                        'product_type': Lightcurve.__tablename__
                    }
                    for lc in new_lc_chunk
                ]
                for kwargs in new_lc_chunk:
                    kwargs['_cadences'] = kwargs['__data'].index
                    for kwarg in ['bjd', 'values', 'errors', 'x_centroids', 'y_centroids', 'quality_flags']:
                        kwargs[f'_{kwarg}'] = kwargs['__data'][kwarg].tolist()
                    del kwargs['__data']

                db.session.bulk_insert_mappings(QLPDataProduct, inheritance_mappings)
                db.session.bulk_insert_mappings(Lightcurve, new_lc_chunk)
                db.session.flush()

                n_inserted += len(new_lc_chunk)

            for update_lc_chunk in chunkify(lightcurve_map.values(), 10000):
                for kwargs in update_lc_chunk:
                    mapping = {
                        'cadences': bindparam('_cadences'),
                        'barycentric_julian_date': bindparam('_bjd')
                    }
                    kwargs['_cadences'] = kwargs['__data'].index
                    kwargs['_bjd'] = kwargs['__data']['bjd'].tolist()
                    for kwarg in ['values', 'errors', 'x_centroids', 'y_centroids', 'quality_flags']:
                        kwargs[f'_{kwarg}'] = kwargs['__data'][kwarg].tolist()
                        mapping[kwarg] = bindparam(f'_{kwarg}')
                    del kwargs['__data']
                update_q = Lightcurve.__table__.update().\
                    where(Lightcurve.id == bindparam('_id')).\
                    values(mapping)
                db.session.execute(update_q, update_lc_chunk)
                db.session.flush()
                n_updated += len(update_lc_chunk)

            db.session.commit()
            logger.info(f'{worker_name}: done')
    return {'n_inserted': n_inserted, 'n_merged': n_updated}


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
        click.echo(f'Pool will process {len(all_tics)} tics')

        if ctx.obj['dryrun']:
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

