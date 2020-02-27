import click
import os
import itertools
import sys
import numpy as np
import tempfile
import csv
import datetime
from random import sample
from functools import partial
from sqlalchemy import Sequence, Column, BigInteger, Integer
from sqlalchemy.sql import func, text, insert
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION
from sqlalchemy.exc import InvalidRequestError
from collections import OrderedDict
from multiprocessing import Pool, cpu_count
from lightcurvedb.core.base_model import QLPModel
from lightcurvedb.models import Aperture, Orbit, LightcurveType, Lightcurve, Lightpoint
from lightcurvedb.core.ingestors.lightcurve_ingestors import h5_to_matrices
from lightcurvedb.core.ingestors.lightpoint import get_raw_h5
from lightcurvedb.util.logging import make_logger
from lightcurvedb.util.iter import partition
from lightcurvedb.util.lightpoint_util import map_existing_lightcurves, create_lightpoint_tmp_table
from lightcurvedb.core.connection import db_from_config
from glob import glob
from h5py import File as H5File
from .base import lcdbcli
from .utils import find_h5
from .types import CommaList

logger = make_logger('H5 Ingestor')

def update_query(tablename):
    return f"""
        UPDATE lightpoints
        SET
            barycentric_julian_date = {tablename}.barycentric_julian_date,
            value = {tablename}.value,
            error = {tablename}.error,
            x_centroid = {tablename}.x_centroid,
            y_centroid = {tablename}.y_centroid,
            quality_flag = {tablename}.quality_flag
        FROM {tablename}
        WHERE
            lightpoints.lightcurve_id = {tablename}.lightcurve_id
            AND
            lightpoints.cadence = {tablename}.cadence
        """

def merge_query(tablename):
    return f"""
        INSERT INTO lightpoints (id, cadence, barycentric_julian_date, value, error, x_centroid, y_centroid, quality_flag, lightcurve_id)
        SELECT nextval('lightpoints_pk_table'), l.cadence, l.barycentric_julian_date, l.value, l.error, l.x_centroid, l.y_centroid, l.quality_flag, l.lightcurve_id
        FROM {tablename} l
        LEFT JOIN lightpoints r
            ON l.lightcurve_id = r.lightcurve_id AND l.cadence = r.cadence
            WHERE r.lightcurve_id IS NULL AND r.cadence IS NULL
"""

def insert_query(tablename):
    return f"""
        INSERT INTO lightpoints (id, cadence, barycentric_julian_date, value, error, x_centroid, y_centroid, quality_flag, lightcurve_id)
        SELECT nextval('lightpoints_pk_table'), l.cadence, l.barycentric_julian_date, l.value, l.error, l.x_centroid, l.y_centroid, l.quality_flag, l.lightcurve_id
        FROM {tablename} l
    """

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


def map_new_lightcurves(new_lc):
    cadence_type, lc_type, aperture, tic = new_lc
    return Lightcurve(
        cadence_type=cadence_type,
        lightcurve_type_id=lc_type,
        aperture_id=aperture,
        tic_id=tic
    )


def ingest_files(config, cadence_type, lc_type_map, aperture_map, table, seq, files):
    pid = os.getpid()
    with db_from_config(config) as db:
        table = QLPModel.metadata.tables[table]
        seq = Sequence(seq)
        tics = {extr_tic(f) for f in files}
        lightcurves = db.lightcurves_from_tics(tics)

        lightcurve_id_map = {
            (lc.cadence_type, lc.lightcurve_type_id, lc.aperture_id, lc.tic_id): lc for lc in lightcurves
        }
        total_points = 0
        for f in files:
            values = []
            new_lightcurves = OrderedDict()
            old_lightcurves = dict()
            h5 = get_raw_h5(f)
            for raw_lc in h5:
                type_id = lc_type_map[raw_lc['lc_type']]
                aperture_id = aperture_map[raw_lc['aperture']]
                tic = raw_lc['tic']
                key = (cadence_type, type_id, aperture_id, tic)
                lc = lightcurve_id_map.get(key, None)

                if not id:
                    target = new_lightcurves
                else:
                    target = old_lightcurves
                    key = lc.id

                if not key in target:
                    target[key] = raw_lc['data']
                else:
                    target[key] = np.concatenate(
                        target[key],
                        raw_lc['data'],
                        axis=1
                    )
            to_insert = [map_new_lightcurves(key) for key in new_lightcurves.keys()]
            db.session.add_all(to_insert)
            if len(to_insert) > 0:
                db.commit()
            # Remap new lightcurves to use new ids
            # outside of session context manager to not hold lock any longer than we need to
            for lightcurve, lightpoints in zip(to_insert, new_lightcurves.values()):
                for lp in lightpoints.T:
                    cache_id = db.session.execute(seq)
                    val = (cache_id, lp[0], lp[1], lp[2], lp[3], lp[4], lp[5], lp[6], lightcurve.id)
                    logger.info(val)
                    values.append(val)
            for id, lightpoints in old_lightcurves.items():
                for lp in lightpoints.T:
                    cache_id = db.session.execute(seq)
                    val = (cache_id, lp[0], lp[1], lp[2], lp[3], lp[4], lp[5], lp[6], id)
                    values.append(val)
            logger.info(f'Worker-{pid} inserting {len(values)} into TMP')
            q = insert(table).values(values)
            db.session.execute(q)
            db.session.commit()
            total_points += len(values)
        return total_points

    #with db_from_config(config, executemany_mode='values') as db:
    #    merge_table = create_lightpoint_tmp_table('merge_cache', QLPModel.metadata)
    #    insert_table = create_lightpoint_tmp_table('insert_cache', QLPModel.metadata)

    #    merge_table.create(bind=db.session.bind)
    #    insert_table.create(bind=db.session.bind)
    #    db.session.commit()
    #    logger.info(f'{pid} created temporary tables')

    #    cache_id = 1
    #    insert_id = 1
    #    logger.info(f'{pid} loading merge table...')
    #    merge_values = []

    #    ids = set(old_lightcurves.keys())
    #    for id in ids:
    #        lightpoint_arr = old_lightcurves.pop(id)
    #        for kwarg in insert_lightpoints_tmp(id, lightpoint_arr):
    #            kwarg['cache_id'] = cache_id
    #            cache_id += 1
    #            merge_values.append(kwarg)

    #        if len(merge_values) >= 5 * 10**5:
    #            q = merge_table.insert()
    #            db.session.execute(q, merge_values)
    #            db.session.flush()
    #            merge_values = []

    #    old_lightcurves = {}

    #    if len(merge_values) > 0:
    #        q = merge_table.insert().values(merge_values)
    #        db.session.execute(q)
    #        db.session.flush()
    #        merge_values = []

    #    db.commit()
    #    
    #    logger.info(f'{pid} creating lightcurve objects')
    #    to_insert = [map_new_lightcurves(key) for key in new_lightcurves.keys()]
    #    db.session.add_all(to_insert)
    #    db.commit()
    #    logger.info(f'{pid} inserted {len(to_insert)} lightcurves')

    #    logger.info(f'{pid} loading insert table')
    #    #  Lightcurve objects now had ids associated with them
    #    insert_values = []
    #    for lightcurve, lightpoints in zip(to_insert, new_lightcurves.values()):
    #        id = lightcurve.id
    #        for kwarg in insert_lightpoints_tmp(id, lightpoints):
    #            kwarg['cache_id'] = insert_id
    #            insert_id += 1
    #            insert_values.append(kwarg)

    #            if len(insert_values) >= 5 * 10**5:
    #                q = insert_table.insert()
    #                db.session.execute(q, insert_values)
    #                db.session.flush()
    #                insert_values = []
    #    
    #    if len(insert_values) > 0:
    #        q = insert_table.insert().values(insert_values)
    #        db.session.execute(q)
    #        db.session.flush()
    #        insert_values = []
    #    
    #    db.commit()
    #    logger.info(f'{pid} loaded {insert_id + cache_id - 1} new lightpoints into TMP')
    #    # All lightpoints have been committed to tables
    #    db.session.execute(text(f'ANALYZE {insert_table.name}'))
    #    db.session.execute(text(f'ANALYZE {merge_table.name}'))
    #    db.commit()
    #    logger.info(f'{pid} analyzed TMP tables')

    #    insertion_q = text(
    #        insert_query(insert_table.name)
    #    )
    #    db.session.execute(insertion_q)
    #    db.commit()
    #    logger.info(f'{pid} inserted new lightpoints')

    #    missing_q = text(
    #        update_query(merge_table.name),
    #    )
    #    db.session.execute(missing_q)
    #    db.commit()
    #    logger.info('f{pid} merged new lighpoints')

    #    merge_q = text(
    #        update_query(merge_table.name)
    #    )
    #    db.session.execute(merge_q)
    #    logger.info(f'{pid} updated lightpoints')

    #    db.commit()
    #    logger.info(f'{pid} finished execution')
    #return insert_id + cache_id - 1
        

def insert_lightpoints_tmp(lightcurve_id, nparray):
    values = []
    for column in nparray.T:
        kwargs = lightpoint_dict(column, lightcurve_id)
        yield kwargs

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
        with Pool(n_process) as p:
            for tic in p.imap_unordered(extr_tic, all_files):
                tics.add(tic)

        click.echo('Found {} unique tics'.format(len(tics)))

        file_partitions = list(partition(all_files, n_process - 1))
        click.echo(
            'Will create {} job partitions:'.format(
                click.style(str(len(file_partitions)), bold=True)
            )
        )

        for i, p in enumerate(file_partitions):
            click.echo('\tPartition {} of length {}'.format(
                click.style('{:4}'.format(i), bold=True, fg='green'),
                click.style('{}'.format(len(p)), bold=True)
            ))

        if not ctx.obj['dryrun']:
            prompt = click.style('Does this information look ok?', bold=True)
            click.confirm(prompt, abort=True)
            click.echo('\tBeginning interpretation of new lightcurves')
        else:
            return

        tmp_table = create_lightpoint_tmp_table('updater', QLPModel.metadata)
        seq = Sequence('cache_id_seq', cache=10**9)
        seq.create(bind=db.session.bind)
        tmp_table.create(bind=db.session.bind)
        db.session.commit()

        func = partial(
            ingest_files,
            ctx.obj['dbconf']._config,
            cadence_type,
            lc_type_map,
            aperture_map,
            tmp_table.name,
            seq.name
        )
        try:
            with Pool(n_process) as p:
                results = p.imap_unordered(
                    func,
                    file_partitions
                )
                values = []
                click.echo(f'Loading temp table: {tmp_table.name}')
                cache_id = 1
                total_points = []
                for result in results:
                    total_points.append(result)
                click.echo('Parsed {sum(total_points)} lightpoints')
                #for result in results:
                #    for lp in result:
                #        cadence, bjd, value, error, x, y, q, lightcurve_id = lp
                #        val = (now, cache_id, lightcurve_id, cadence, bjd, value, error, x, y, q)
                #        values.append(val)
                #        cache_id += 1
                #    if len(values) > 10**6:
                #        click.echo(f'Batch loading {len(values)} lightpoints...')
                #        q = tmp_table.insert().values(values)
                #        db.session.execute(q)
                #        db.session.flush()
                #        values = []
                ## Flush any remaining values
                #if len(values) > 0:
                #    click.echo(f'Batch loading {len(values)} lightpoints...')
                #    q = tmp_table.insert().values(values)
                #    db.session.execute(q)
                #    db.session.flush()
            click.echo('Analyzing cache table')
            db.session.execute(f'ANALYZE {tmp_table.name}')
            db.session.commit()
            click.echo('Performing MERGE')
            # Perform updates to lightcurve database
            merge_q = merge_query(tmp_table.name)
            db.session.execute(merge_q)
            db.session.commit()
            click.echo('Done')
        except:
            tmp_table.drop(bind=db.session.bind)
            seq.drop(bind=db.session.bind)
            db.session.commit()
            raise

            #with tempfile.NamedTemporaryFile(mode='w', dir=ctx.obj['scratch']) as csv_out:
            #    click.echo(f'Serializing to csv {csv_out.name}')
            #    writer = csv.writer(csv_out, delimiter=',', quoting=csv.QUOTE_NONNUMERIC)
            #    writer.writerow(
            #        ['created',
            #        'cadence',
            #        'barycentric_julian_date',
            #        'value',
            #        'error',
            #        'x_centroid',
            #        'y_centroid',
            #        'quality_flag',
            #        'lightcurve_id',
            #        'id']
            #    )
            #    for result in results:
            #        for lp in result:
            #            val = (now, *lp, current_lightpoint_id)
            #            writer.writerow(val)
            #            current_lightpoint_id += 1

            #    fullpath = csv_out.name
            #    click.echo('Performing COPY psql command...')
            #    resultant = db.session.execute(
            #        text('COPY lightcurves FROM \'{fullpath}\' WITH HEADER')
            #    )
            #    click.echo(f'Done: {resultant}')
