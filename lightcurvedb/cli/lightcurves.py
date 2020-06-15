import click
import os
import itertools
import sys
import numpy as np
import logging
import re
import pandas as pd
from tabulate import tabulate
from random import sample
from functools import partial
from sqlalchemy.sql.expression import bindparam
from sqlalchemy.sql import update
from sqlalchemy.dialects.postgresql import insert
from multiprocessing import Pool, cpu_count
from lightcurvedb.models import Aperture, Orbit, LightcurveType, Lightcurve, Observation
from lightcurvedb.core.ingestors.lightcurve_ingestors import h5_to_kwargs, LightpointCache, TempLightcurveIDMapper, lc_dict_to_df, parallel_h5_merge
from lightcurvedb.core.ingestors.quality_flag_ingestors import QualityFlagReference
from lightcurvedb.managers.lightcurve_query import LightcurveManager
from lightcurvedb.util.iter import partition, chunkify, partition_by
from lightcurvedb.core.connection import db_from_config
from h5py import File as H5File
from .base import lcdbcli
from .utils import find_h5, extr_tic, group_h5
from .types import CommaList


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

logger.debug('Initialized logger')

COLS = [
    'cadences',
    'barycentric_julian_date',
    'values',
    'errors',
    'x_centroids',
    'y_centroids',
    'quality_flags'
]


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
    match = re.search(search, h5path)
    if match:
        return match.groupdict()
    return None

def yield_lightcurves_from(file_groups):
    for h5 in itertools.chain.from_iterable(file_groups):
        for lc in h5_to_kwargs(h5):
            yield lc, h5

def make_key(kwargs):
    return (kwargs['tic_id'], kwargs['aperture_id'], kwargs['lightcurve_type_id'])


def conv_lc_dict(kwargs):
    for col in COLS:
        kwargs[col] = kwargs[col].tolist()
    return kwargs

@lightcurve.command()
@click.pass_context
@click.argument('orbits', type=int, nargs=-1)
@click.option('--n-process', type=int, default=-1, help='Number of cores. <= 0 will use all cores available')
@click.option('--cameras', type=CommaList(int), default='1,2,3,4')
@click.option('--ccds', type=CommaList(int), default='1,2,3,4')
@click.option('--orbit-dir', type=click.Path(exists=True, file_okay=False), default='/pdo/qlp-data')
@click.option('--scratch', type=click.Path(exists=True, file_okay=False), default='/scratch/')
def ingest_h5(ctx, orbits, n_process, cameras, ccds, orbit_dir, scratch):
    temp_ids = TempLightcurveIDMapper()
    pid = os.getpid()
    uri = os.path.join(scratch, f'{pid}-lightpoints.db')
    lightpoints = LightpointCache()
    current_tmp_id = -1
    with ctx.obj['dbconf'] as db:
        orbits = db.orbits.filter(Orbit.orbit_number.in_(orbits)).all()
        orbit_numbers = [o.orbit_number for o in orbits]
        orbit_map = {
            orbit.orbit_number: orbit.id for orbit in orbits
        }
        apertures = db.apertures.all()
        lc_types = db.lightcurve_types.all()

        qflag_ref = QualityFlagReference()
        for orbit_n in orbit_numbers:
            qflag_ref.ingest(orbit_n)

        click.echo(
            'Ingesting {} orbits with {} apertures'.format(len(orbits), len(apertures))
        )

        if n_process <= 0:
            n_process = cpu_count()

        click.echo(
            'Utilizing {} cores'.format(click.style(str(n_process), bold=True))
        )

        paths = [determine_orbit_path(orbit_dir, o, cam, ccd) for o, cam, ccd in itertools.product(orbit_numbers, cameras, ccds)]
        grouped_lcs = list(group_h5(find_h5(*paths)))

        tics = {group[0] for group in grouped_lcs}
        click.echo(f'Will process {len(tics)} TICs')

        for tic_chunk in chunkify(tics, 10000):
            q = db.lightcurves_from_tics(tic_chunk)
            observations = []
            observed_tics = set()
            logger.debug(f'Executing query for existing lightcurves')
            for lightcurve in q.yield_per(100):
                logger.debug(f'Loading {lightcurve}, length: {len(lightcurve)}')
                lightpoints.ingest_lc(lightcurve)
                temp_ids.set_id(
                    lightcurve.id,
                    lightcurve.tic_id,
                    lightcurve.aperture_id,
                    lightcurve.lightcurve_type_id
                )

            file_groups = [group for _, group in filter(lambda g: g[0] in tic_chunk, grouped_lcs)]

            new_lcs = []

            with Pool(n_process) as p:
                results = p.imap_unordered(parallel_h5_merge, file_groups)
                for lc_observed_in, merged_lc_kwargs in results:
                    observations += lc_observed_in
                    for merged_lc_kwarg in merged_lc_kwargs:
                        id_check = temp_ids.get_id_by_dict(merged_lc_kwarg)
                        if id_check is None:
                            temp_ids.set_id(
                                current_tmp_id,
                                merged_lc_kwarg['tic_id'],
                                merged_lc_kwarg['aperture_id'],
                                merged_lc_kwarg['lightcurve_type_id']
                            )
                            id_check = current_tmp_id
                            current_tmp_id -= 1
                            new_lcs.append(
                                conv_lc_dict(merged_lc_kwarg)
                            )
                        else:
                            # Emplace
                            lightpoints.ingest_dict(merged_lc_kwarg, id_check)
                    logger.info(f'Processed {lc_observed_in[0]["tic_id"]}')

            lightcurve_q = insert(Lightcurve.__table__)
            observation_q = insert(Observation.__table__).on_conflict_do_nothing()

            for observation in observations:
                observation['orbit_id'] = orbit_map[int(observation['orbit'])]
                del observation['orbit']

            logger.info(f'Performing insertion of {len(new_lcs)} new lightcurves')
            for data_chunk in chunkify(new_lcs, 100):
                db.session.execute(lightcurve_q, data_chunk)
            logger.info(f'Performing insertion of {len(observation)} tic observation maps')
            db.session.execute(observation_q, observations)

            logger.info(f'Performing update query construction')
            q = update(Lightcurve.__table__).where(Lightcurve.id == bindparam('_id')).values({
                'cadences': bindparam('cadences'),
                'barycentric_julian_date': bindparam('barycentric_julian_date'),
                'values': bindparam('values'),
                'errors': bindparam('errors'),
                'x_centroids': bindparam('x_centroids'),
                'y_centroids': bindparam('y_centroids'),
                'quality_flags': bindparam('quality_flags')
                })
            merging_lc_ids = lightpoints.get_lightcurve_ids()
            logger.info(f'Updating {len(merging_lc_ids)} lightcurves')
            for ids in chunkify(merging_lc_ids, 100):
                db.session.execute(q, list(lightpoints.yield_insert_kwargs(ids)))

        db.commit()
