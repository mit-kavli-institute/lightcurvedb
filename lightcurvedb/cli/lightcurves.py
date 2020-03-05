import click
import os
import itertools
import sys
import numpy as np
import tempfile
import csv
import datetime
import io
from random import sample
from functools import partial
from sqlalchemy import Sequence, Column, BigInteger, Integer
from sqlalchemy.sql import func, text, insert, select, func
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION
from sqlalchemy.exc import InvalidRequestError
from sqlalchemy.orm import joinedload
from collections import OrderedDict, defaultdict
from multiprocessing import Pool, cpu_count, SimpleQueue, Manager
from lightcurvedb.core.base_model import QLPModel
from lightcurvedb.models import Aperture, Orbit, LightcurveType, Lightcurve, Lightpoint
from lightcurvedb.core.ingestors.lightcurve_ingestors import h5_to_matrices
from lightcurvedb.core.ingestors.lightpoint import get_raw_h5, get_cadence_info
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


def map_new_lightcurves(new_lc):
    tic, lc_type, aperture, cadence_type = new_lc
    return Lightcurve(
        cadence_type=cadence_type,
        lightcurve_type_id=lc_type,
        aperture_id=aperture,
        tic_id=tic
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
    for raw_lc in h5:
        type_id = lc_type_map[raw_lc['lc_type']]
        aperture_id = aperture_map[raw_lc['aperture']]
        tic = raw_lc['tic']
        data = raw_lc['data']
        length = data.shape[1]
        key = (tic, type_id, aperture_id, cadence_type)
        lc_id = lightcurve_id_map.get(key, None)
        tmp = np.vstack((
            np.full(length, tic),
            np.full(length, type_id),
            np.full(length, aperture_id),
            data,
            np.full(length, lc_id)
        ))
        if values is None:
            values = tmp
        else:
            values = np.concatenate((values, tmp), axis=1)

    return values

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
        lightcurves = db.lightcurves_from_tics(tics).all()
        click.echo(f'Found {len(lightcurves)} lightcurves from {len(tics)} tics, creating mappings')
        cadence_agg = func.array_agg(Lightpoint.cadence).label('cadences')
        q = db.session.query(Lightcurve.id.label('id'), cadence_agg).innerjoin(
                Lightpoint,
                Lightpoint.lightcurve_id == Lightcurve.id
            ).where(Lightcurve.tic_id.in_(tics)).group_by(Lightcurve.id)
        lightcurve_id_map = {}
        lightcurve_map = {}
        cadence_map = {}
        with click.progressbar(lightcurves) as i:
            for lc in i:
                key = make_lc_key(lc)
                lightcurve_id_map[key] = lc.id
                lightcurve_map[key] = lc.id
                cadence_map = set(lc.cadences)

        with click.progressbar(q.all()) as cadence_mapping:
            for cadence_check in cadence_mapping:
                id = cadence_check[0]
                cadences = set(cadence_check[1])
                cadence_map[id] = cadences

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
        total_points = 0
        new_lightcurves = {}

        try:
            with Pool(n_process) as p:
                results = p.imap_unordered(
                    func,
                    all_files
                )
                cache_id = 1
                with tempfile.NamedTemporaryFile(mode='w', dir=ctx.obj['scratch'], delete=False) as csv_out:
                    click.echo(f'Serializing to {csv_out.name}')
                    fieldnames = ['cache_id', 'cadence', 'barycentric_julian_date', 'value', 'error', 'x_centroid', 'y_centroid', 'quality_flag', 'lightcurve_id']
                    writer = csv.DictWriter(csv_out, fieldnames, delimiter=',', quoting=csv.QUOTE_NONNUMERIC)
                    for result in results:
                        # Result is one large numpy block
                        points = []
                        ignored = 0
                        added = 0
                        for raw_lightpoint in result.T:
                            key, data = expand_raw_lp(raw_lightpoint, cadence_type)

                            if should_skip(cadence_map, lightcurve_map, data['cadence'], data['lightcurve_id']):
                                # Cadence already parsed/exists in db
                                ignored += 1
                                continue

                            if data['lightcurve_id'] is not None:
                                id = data['lightcurve_id']
                                if id not in cadence_map:
                                    if id in lightcurve_map:
                                        cadence_map[id] = set(
                                            lightcurve_map[id].cadences
                                        )
                            elif key in new_lightcurves:
                                # Fallback
                                data['lightcurve_id'] = new_lightcurves[key]
                            else:
                                # Novel lightcurve
                                lc = map_new_lightcurves(key)
                                db.add(lc)
                                new_lightcurves[key] = lc.id
                                data['lightcurve_id'] = lc.id
                                cadence_map[lc.id] = set()

                            writer.writerow(data)

                            # Update cache ids
                            data['cache_id'] = cache_id
                            cache_id += 1
                            added += 1

                            # Update which cadences we've seen
                            if data['lightcurve_id'] not in cadence_map:
                                cadence_map[data['lightcurve_id']] = set()
                            cadence_map[data['lightcurve_id']].add(data['cadence'])
                        click.echo(f'Found {added} new points, ignoring {ignored}')

                        # Insert points into cache
                        #click.echo(f'Dumping {len(points)} points to cache')
                        #q = cache_table.insert().values(
                        #    points
                        #)
                        #db.session.execute(q)
                    # Save changes to cache
                    #db.session.commit()
            #click.echo('Done parsing files into cache')
            #insertion_q = insert_query(cache_table.name)
            #click.echo('Commiting changes to database...')
            #db.session.execute(insertion_q)
            #db.session.commit()
            #click.echo('Done')
            # Create tmp table
            # cache_table = create_lightpoint_tmp_table(
            #     'lightpoint_cache',
            #     QLPModel.metadata
            # )
            # cache_table.create(bind=db.session.bind)
            # db.session.commit()

        except:
            db.session.rollback()
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
