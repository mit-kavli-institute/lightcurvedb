import click
import os
import itertools
import sys
import numpy as np
from random import sample
from functools import partial
from sqlalchemy import Sequence, Column, BigInteger, Integer
from sqlalchemy.sql import func, text
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION
from sqlalchemy.exc import InvalidRequestError
from collections import OrderedDict
from multiprocessing import Pool, cpu_count
from lightcurvedb.core.base_model import QLPModel
from lightcurvedb.models import Aperture, Orbit, LightcurveType, Lightcurve, Lightpoint
from lightcurvedb.core.ingestors.lightcurve_ingestors import h5_to_matrices
from lightcurvedb.core.ingestors.lightpoint import get_raw_h5
from lightcurvedb.util.logging import make_logger
from lightcurvedb.util.iter import chunkify, enumerate_chunkify
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

def insert_query(tablename):
    return f"""
        INSERT INTO lightpoints (id, cadence, barycentric_julian_date, value, error, x_centroid, y_centroid, quality_flag, lightcurve_id)
        SELECT nextval('lightpoints_pk_table'), l.cadence, l.barycentric_julian_date, l.value, l.error, l.x_centroid, l.y_centroid, l.quality_flag, l.lightcurve_id
        FROM {tablename} l
        LEFT JOIN lightpoints r
            ON l.lightcurve_id = r.lightcurve_id AND l.cadence = r.cadence
            WHERE r.lightcurve_id IS NULL AND r.cadence IS NULL
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


def expand(x):
    return (
        (
            x.cadence_type,
            x.lightcurve_type_id,
            x.aperture_id,
            x.tic_id
        ),
        x
    )

def lightpoint_dict(T, lc_id):
    return {
        'cadence': T[0],
        'barycentric_julian_date': T[1],
        'value': T[2],
        'error': T[3],
        'x_centroid': T[4],
        'y_centroid': T[5],
        'quality_flag': T[6],
        'lightcurve_id': lc_id
    }

def yield_lightpoint_caches(lightpoint_map, cache_id_offset=1):
    current_id = cache_id_offset
    for lightcurve_id, lightpoint_array in lightpoint_map.items():
        for lightpoint in lightpoint_array.T:
            
            kwarg = lightpoint_dict(lightpoint, lightcurve_id)
            kwarg['cache_id'] = current_id
            current_id += 1
            yield kwarg

def extr_for_lightpoint(cache_tuple):
    lightcurve_id = cache_tuple[0]
    points = []
    for lightpoint in cache_tuple[1].T:
        kwarg = lightpoint_dict(lightpoint, lightcurve_id)
        points.append(kwarg)
    return points


def yield_lightpoints(collection, lc_id_map, merging=False):
        for lc_id, lcs in collection.items():
            for lc in lcs:
                for lp in lc.T:
                    yield lightpoint_dict(lp, lc_id)


def extract_lightpoints(lightcurves):
    for lightcurve, lightpoint_arr in lightcurves.items():
        lightpoints = []
        for lightpoint in lightpoint_arr.T:
            kwarg = lightpoint_dict(lightpoint, lightcurve.id)
            kwarg['id'] = Sequence('lightpoints_pk_table')
            lightpoints.append(lightpoint)
        yield lightpoints


def insert_lightpoints(config, lightpoint_kwargs):
    with db_from_config(config) as db:
        db.session.bulk_insert_mappings(
            Lightpoint,
            lightpoint_kwargs
        )

def insert_lightcurves(config, lightcurves):
    with db_from_config(config) as db:
        for lc in lightcurves:
            if lc is None:
                continue
            db.add(lc)
        db.commit()
    return lightcurves

def make_merge_stmt(points):
    q = insert(Lightcurve).values(
        points
    ).on_confict_do_update(
        constraint='lc_cadence_unique'
    )
    return q


def merge_lightpoints(config, dict_items):
    pid = os.getpid()
    with db_from_config(config) as db:
        values = []
        cache_id = 1
        for lightcurve_id, data in dict_items:
            for lp in data.T:
                values.append(lightpoint_dict(lp, lightcurve_id))
                values[-1]['cache_id'] = cache_id
                cache_id += 1
        table = create_lightpoint_tmp_table('lp_cache')
        table.create(bind=db.session.bind)
        logger.info(f'{pid} created tmp table {table.name}')
        db.commit()
        db.session.execute(text(f'ANALYZE {table.name}'))
        db.commit()
        for v in values:
            v['cache_id'] = cache_id
            cache_id += 1
        q = table.insert().values(values)
        db.commit()
        db.session.execute(q)
        q = text(
            update_query(table.name)
        )
        db.session.execute(q)
        logger.info(f'{pid} updated database')
        q = text(
            insert_query(table.name)
        )
        db.session.execute(q)
        db.commit()
        logger.info(f'{pid} inserted into database')
        return len(values)


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

        click.echo('Performing join on existing lightcurves')
        # Get a mapping of (cadence, type, aperture, tic) -> pk
        lightcurve_id_map = map_existing_lightcurves(db, tics)


        new_lightcurves = OrderedDict()
        old_lightcurves = dict()

        with Pool(n_process) as p:
            with click.progressbar(all_files, label='Reading H5 files') as file_iter:
                for result in p.imap_unordered(get_raw_h5, file_iter):
                    for raw_lc in result:
                        type_id = lc_type_map[raw_lc['lc_type']]
                        aperture_id  = aperture_map[raw_lc['aperture']]
                        tic = raw_lc['tic']
                        key = (cadence_type, type_id, aperture_id, tic)
                        try:
                            existing_lc_id = lightcurve_id_map[key]
                            if existing_lc_id not in old_lightcurves:
                                old_lightcurves[existing_lc_id] = raw_lc['data']
                            else:
                                old_lightcurves[existing_lc_id] = np.concatenate(
                                    [
                                        old_lightcurves[existing_lc_id],
                                        raw_lc['data']
                                    ],
                                    axis=1
                                )

                        except KeyError:
                            # Key does not exist, lightcurve must be new
                            if key not in new_lightcurves:
                                new_lightcurves[key] = raw_lc['data']
                            else:
                                new_lightcurves[key] = np.concatenate(
                                    [
                                        new_lightcurves[key],
                                        raw_lc['data']
                                    ],
                                    axis=1
                                )

        click.echo(
            click.style(
                'Will merge {} lightcurves'.format(len(old_lightcurves)),
                fg='yellow',
                bold=True
            )
        )
        click.echo(
            click.style(
                'Will insert {} lightcurves'.format(len(new_lightcurves)),
                fg='green',
                bold=True
            )
        )

        if not ctx.obj['dryrun']:
            prompt = click.style('Do these changes look ok?', bold=True)
            click.confirm(prompt, abort=True)
            click.echo('\tBeginning interpretation of new lightcurves')
        config = ctx.obj['dbconf']._config

        # Numpy array representing the full lightcurve are now in
        # new and old lightcurve dictionaries
        click.echo('Mapping new lightcurves into Lightcurve Object instances')
        with Pool(n_process) as p:
            lightcurves = p.map(map_new_lightcurves, new_lightcurves.keys()) # Remap lightcurves to the new_lightcurve dictionary
            lightcurve_batches = chunkify(lightcurves, 100000)
            click.echo('\tInserting lightcurve instances into Database')
            func = partial(insert_lightcurves, ctx.obj['dbconf']._config)
            lightcurves = p.imap(
                func,
                lightcurve_batches
            )
            new_lightcurves = dict(zip(lightcurves, new_lightcurves.values()))

            click.echo('\tInserted new lightcurves')

            # Insert lightpoints
            click.echo('\tInserting lightpoints')
            # get iterator over lightpoints
            # creating a list would result in duplicate memory usage
            func = partial(insert_lightpoints, ctx.obj['dbconf'])
            lp_with_id = extract_lightpoints(new_lightcurves)
            p.imap_unordered(func, lp_with_id, chunksize=10000)

        # Update lightpoint PK sequence

        click.echo('Inserted, now determining merging strategy')
        with Pool(n_process) as p:
            # Load a Temporary Table with lightcurves to merge
            # Perform query of relevant existing lightpoints
            click.echo('\tInserting conflicting lightpoints into a TEMP tables')
            #lightpoint_iter = p.imap_unordered(
            #        extr_for_lightpoint,
            #        old_lightcurves.items()
            #)
            #lightpoint_iter = itertools.chain.from_iterable(
            #    lightpoint_iter
            #)
            lightpoint_iter = chunkify(old_lightcurves.items(), 10**2)
            func = partial(merge_lightpoints, str(ctx.obj['dbconf']._config))
            click.echo('Delegating merge operations to processes')
            result_iterator = p.imap_unordered(
                func,
                lightpoint_iter,
            )
            insertions = [x for x in result_iterator]
            result = sum(insertions)
            click.echo(
                'Inserted {} lightpoints'.format(
                    click.style(str(result), bold=True)
                )
            )
        click.echo('Done')
