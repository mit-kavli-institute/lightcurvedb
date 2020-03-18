import click
import os
import itertools
import sys
import numpy as np
import tempfile
import csv
import datetime
import io
from datetime import datetime
from random import sample
from functools import partial
from sqlalchemy import Sequence, Column, BigInteger, Integer
from sqlalchemy.sql import func, text, insert, select, func
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION
from sqlalchemy.exc import InvalidRequestError
from sqlalchemy.orm import joinedload
from collections import OrderedDict, defaultdict
from multiprocessing import Pool, cpu_count, SimpleQueue, Manager
from lightcurvedb.core.base_model import QLPModel, QLPDataProduct
from lightcurvedb.models import Aperture, Orbit, LightcurveType, LightcurveRevision
from lightcurvedb.core.ingestors.lightcurve_ingestors import h5_to_matrices
from lightcurvedb.core.ingestors.lightpoint import get_raw_h5, get_cadence_info
from lightcurvedb.util.logging import make_logger
from lightcurvedb.util.iter import partition, chunkify
from lightcurvedb.util.merge import matrix_merge
from lightcurvedb.util.lightpoint_util import map_existing_lightcurves, create_lightpoint_tmp_table
from lightcurvedb.reportings.lightcurve_ingest_report import IngestReport
from lightcurvedb.core.connection import db_from_config
from glob import glob
from h5py import File as H5File
from .base import lcdbcli
from .utils import find_h5
from .types import CommaList

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

def expand_raw_lp(lightpoint_col, cadence_type):
    key = (
        lightpoint_col[0],
        lightpoint_col[1],
        lightpoint_col[2],
        cadence_type
    )
    return (
        key,
        {
            'cadence': int(lightpoint_col[3]),
            'barycentric_julian_date': lightpoint_col[4],
            'value': lightpoint_col[5],
            'error': lightpoint_col[6],
            'x_centroid': lightpoint_col[7],
            'y_centroid': lightpoint_col[8],
            'quality_flag': int(lightpoint_col[9]),
            'lightcurve_id': int(lightpoint_col[10]) if lightpoint_col[10] is not None else None
        }
    )


def should_skip(cadence_map, lightcurve_map, cadence, lc_id):
    if lc_id is None:
        return False
    if not lc_id in cadence_map:
        if not lc_id in lightcurve_map:
            return False
        cadence_map[lc_id] = set(lightcurve_map[lc_id].cadences)
    return cadence in cadence_map[lc_id]


def ingest_files(cadence_type, lc_type_map, aperture_map, lightcurve_id_map, f):
    pid = os.getpid()
    values = None
    h5 = get_raw_h5(f)
    result = []
    for raw_lc in h5:
        type_id = lc_type_map[raw_lc['lc_type']]
        aperture_id = aperture_map[raw_lc['aperture']]
        tic = raw_lc['tic']
        data = raw_lc['data']
        length = data.shape[1]
        key = (tic, type_id, aperture_id, cadence_type)
        lc_id = lightcurve_id_map.get(key, None)

        result.append({
            'tic_id': tic,
            'aperture_id': aperture_id,
            'lightcurve_type_id': type_id,
            'data': data,
            'lightcurve_id': lc_id
        })
    return result

def insert_lightpoints_tmp(lightcurve_id, nparray):
    values = []
    for column in nparray.T:
        kwargs = lightpoint_dict(column, lightcurve_id)
        yield kwargs

def merge_lc_dict(lc_dict, new_data, new=False):
    if new:
        LC_KWARGS = ['cadences', 'barycentric_julian_date', 'values', 'errors', 'x_centroids', 'y_centroids', 'quality_flags']
    else:
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

        all_files = list(itertools.chain(*path_iterators))
        tics = set()

        # Load defined tics
        click.echo(
            'Extracting defined tics from {} files'.format(str(len(all_files)))
        )
        qlp_ids = set(v[0] for v in db.session.query(QLPDataProduct.id))
        click.echo(f'Found {len(qlp_ids)} existing qlpdataproduct ids')
        lightcurve_id_map = {}
        lightcurve_map = {}
        cadence_map = {}
        inheritance_mappings = []

        with Pool(n_process) as p:
            for tic in p.imap_unordered(extr_tic, all_files):
                tics.add(tic)

        click.echo('Found {} unique tics'.format(len(tics)))
        lightcurve_q = db.session.query(LightcurveRevision).filter(LightcurveRevision.tic_id.in_(tics))
        click.echo('Mapping lightcurves...')
        for lightcurve in lightcurve_q.yield_per(10000):
            click.echo(f'\tWill merge {lightcurve}')
            lc = lightcurve
            key = make_lc_key(lc)
            lightcurve_id_map[key] = lc.id
            lightcurve_map[lc.id] = {
                'id': lc.id,
                '_cadences': lc.cadences,
                '_bjd': lc.bjd,
                '_values': lc.values,
                '_errors': lc.errors,
                '_x_centroids': lc.x_centroids,
                '_y_centroids': lc.y_centroids,
                '_quality_flags': lc.quality_flags
            }

        if not ctx.obj['dryrun']:
            prompt = click.style('Does this information look ok?', bold=True)
            click.confirm(prompt, abort=True)
            click.echo('\tBeginning interpretation of new lightcurves')
        else:
            return

        func = partial(
            ingest_files,
            cadence_type,
            lc_type_map,
            aperture_map,
            lightcurve_id_map
        )
        new_lightcurves = 0
        merged_lightcurves = 0
        new_lightcurves = {}
        qlpdp_id = 1

        try:
            with Pool(n_process) as p:
                click.echo('\tPreparing worker pool')
                results = p.imap_unordered(
                    func,
                    all_files
                )
                click.echo('\tExecuting pool...')
                import_start_time = datetime.now()
                for lc_list in results:
                    for result in lc_list:
                        id = result['lightcurve_id']
                        if id is None:
                            # New lightcurve, possible merge
                            tic = result['tic_id']
                            lc_type = result['lightcurve_type_id']
                            aperture = result['aperture_id']
                            data = result['data']
                            key = (tic, lc_type, aperture, cadence_type)
                            if key in new_lightcurves:
                                prev_data = new_lightcurves[key]
                                merge_lc_dict(prev_data, data, new=True)
                            else:
                                # Make a new data product
                                while qlpdp_id in qlp_ids:
                                    qlpdp_id += 1
                                qlpdp = {
                                    'id': qlpdp_id,
                                    'product_type': LightcurveRevision.__tablename__,
                                    'created_on': datetime.now()
                                }

                                new_lightcurves[key] = {
                                    'id': qlpdp['id'],
                                    'tic_id': tic,
                                    'lightcurve_type_id': lc_type,
                                    'aperture_id': aperture,
                                    'cadence_type': cadence_type,
                                    'cadences': data[0],
                                    'barycentric_julian_date': data[1],
                                    'values': data[2],
                                    'errors': data[3],
                                    'x_centroids': data[4],
                                    'y_centroids': data[5],
                                    'quality_flags': data[6]
                                }
                                qlp_ids.add(qlpdp_id)
                                logger.info(f'New lightcurve {tic}[{qlpdp_id}]')
                                inheritance_mappings.append(qlpdp)
                        else:
                            # Found existing LC, definite merge
                            lc_data = lightcurve_map[id]
                            logger.info(f'Merging {lc.tic_id}[{lc.id}]')
                            lightcurve_map[id] = merge_lc_dict(
                                lightcurve_map[id],
                                result['data']
                            )
            import_end_time = datetime.now()
            click.echo(f'Submitting')
            mapping_start_time = datetime.now()
            click.echo('Inserting qlpdataproducts')
            for chunk in chunkify(inheritance_mappings, 10000):
                db.session.bulk_insert_mappings(QLPDataProduct, chunk)
            click.echo('Inserting {len(new_lightcurves)} Lightcurves')
            q = LightcurveRevision.__table__.insert().values(new_lightcurves.values())
            db.session.execute(q)
            mapping_end_time = datetime.now()
            click.echo('Updating {len(lightcurve_map)} Lightcurves')
            for chunk in chunkify(lightcurve_map.values(), 10000):
                db.session.bulk_update_mappings(LightcurveRevision, chunk)
            click.echo('Cleaning up and syncing ID sequences')
            db.session.execute(text(
                f'ALTER SEQUENCE qlpdataproducts_pk_table RESTART WITH {qlpdp_id}'
            ))
            click.echo('Committing')
            db.session.commit()
            ingest_end = datetime.now()
            click.echo('Done')
            report = IngestReport(
                import_start_time,
                ingest_end, 
                import_end_time - import_start_time,
                mapping_end_time - mapping_start_time,
                ingest_end - mapping_end_time,
                lightcurve_map,
                new_lightcurves)
            print(report)

        except:
            db.session.rollback()
            raise
