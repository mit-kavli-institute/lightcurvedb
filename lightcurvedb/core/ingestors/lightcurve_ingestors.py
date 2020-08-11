try:
    # Python agnostic import of Queue
    import queue
except ImportError:
    import Queue as queue

import warnings
with warnings.catch_warnings():
    warnings.simplefilter('ignore', category=FutureWarning)
    from h5py import File as H5File

from collections import deque
from math import ceil
from sqlalchemy import Sequence, Column, BigInteger, Integer, func, text, Table, String, select, bindparam, join
from sqlalchemy.orm.session import Session, sessionmaker
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION, insert
from lightcurvedb.core.base_model import QLPModel
from lightcurvedb.models import Aperture, LightcurveType, Lightcurve, Observation, Orbit, Frame, QLPProcess, QLPAlteration
from lightcurvedb.util.iter import chunkify
from lightcurvedb.util.logger import lcdb_logger as logger
from lightcurvedb.util.merge import merge_arrays
from lightcurvedb.core.tic8 import TIC8_ENGINE, TIC_Entries
from lightcurvedb.core.ingestors.quality_flag_ingestors import get_qflag_df, update_qflag
from lightcurvedb.core.ingestors.cache import IngestionCache

from lightcurvedb.legacy.timecorrect import TimeCorrector
from lightcurvedb import db_from_config, LightcurveManager
import numpy as np
import pandas as pd
import os
import re
from datetime import datetime
from itertools import product
from .temp_table import LightcurveIDMapper, TempObservation, IngestionJob, TIC8Parameters


THRESHOLD = 1 * 10**9 / 4  # bytes


path_components = re.compile(r'orbit-(?P<orbit>[1-9][0-9]*)/ffi/cam(?P<camera>[1-4])/ccd(?P<ccd>[1-4])/LC/(?P<tic>[1-9][0-9]*)\.h5$')


def quality_flag_extr(qflags):
    accept = np.ones(qflags.shape[0], dtype=np.int64)
    for i in range(qflags.shape[0]):
        if qflags[i] == b'G':
            accept[i] = 1
        else:
            accept[i] = 0
    return accept

# Def: KEY -> Has error field
H5_LC_TYPES = {
    'KSPMagnitude': False,
    'RawMagnitude': True
}

def h5_to_observation(filepath):
    context = path_components.search(filepath).groupdict()
    mapped = dict(context)
    mapped['tic_id'] = mapped['tic']
    del mapped['tic']
    return mapped


def h5_to_matrices(filepath):
    with H5File(filepath, 'r') as h5in:
        # Iterate and yield extracted h5 interior data
        lc = h5in['LightCurve']
        tic = int(os.path.basename(filepath).split('.')[0])
        cadences = lc['Cadence'][()]
        bjd = lc['BJD'][()]

        apertures = lc['AperturePhotometry'].keys()
        for aperture in apertures:
            compound_lc = lc['AperturePhotometry'][aperture]
            x_centroids = compound_lc['X'][()]
            y_centroids = compound_lc['Y'][()]
            quality_flags = quality_flag_extr(compound_lc['QualityFlag'][()])
            for lc_type, has_error in H5_LC_TYPES.items():
                result = {
                    'lc_type': lc_type,
                    'aperture': aperture,
                    'tic': tic,
                }
                values = compound_lc[lc_type][()]

                if has_error:
                    errors = compound_lc['{}Error'.format(lc_type)][()]
                else:
                    errors = np.full_like(cadences, np.nan, dtype=np.double)

                result['data'] = np.array([
                    cadences,
                    bjd,
                    values,
                    errors,
                    x_centroids,
                    y_centroids,
                    quality_flags
                ])

                yield result


def h5_to_kwargs(filepath, **constants):
    with H5File(filepath, 'r') as h5in:
        lc = h5in['LightCurve']
        tic = int(os.path.basename(filepath).split('.')[0])
        cadences = lc['Cadence'][()].astype(int)
        bjd = lc['BJD'][()]
        apertures = lc['AperturePhotometry'].keys()

        for aperture in apertures:
            lc_by_ap = lc['AperturePhotometry'][aperture]
            x_centroids = lc_by_ap['X'][()]
            y_centroids = lc_by_ap['Y'][()]
            quality_flags = quality_flag_extr(lc_by_ap['QualityFlag'][()]).astype(int)

            for lc_type, has_error_field in H5_LC_TYPES.items():
                if not lc_type in lc_by_ap:
                    continue
                values = lc_by_ap[lc_type][()]
                if has_error_field:
                    errors = lc_by_ap['{}Error'.format(lc_type)][()]
                else:
                    errors = np.full_like(cadences, np.nan, dtype=np.double)

                yield dict(
                    tic_id=tic,
                    lightcurve_type_id=lc_type,
                    aperture_id=aperture,
                    cadences=cadences,
                    barycentric_julian_date=bjd,
                    values=values,
                    errors=errors,
                    x_centroids=x_centroids,
                    y_centroids=y_centroids,
                    quality_flags=quality_flags,
                    **constants
                )


def lc_dict_to_df(dictionary, **constants):

    df = pd.DataFrame(
         {
            'cadence': dictionary['cadences'],
            'barycentric_julian_date': dictionary['barycentric_julian_date'],
            'value': dictionary['values'],
            'error': dictionary['errors'],
            'x_centroid': dictionary['x_centroids'],
            'y_centroid': dictionary['y_centroids'],
            'quality_flag': dictionary['quality_flags'],
            'orbit_number': dictionary['orbit_number'],
            'camera': dictionary['camera'],
            'ccd': dictionary['ccd']
        }
    )
    for name, value in constants.items():
        df[name] = value
    return df


def load_quality_flags(*orbit_numbers):
    dfs = []
    for orbit, camera, ccd in product(orbit_numbers, [1,2,3,4], [1,2,3,4]):
        path = os.path.join(
            '/','pdo', 'qlp-data',
            'orbit-{}'.format(orbit),
            'ffi',
            'run'
        )
        filename = 'cam{}ccd{}_qflag.txt'.format(camera, ccd)
        full_path = os.path.join(path, filename)
        q_df = pd.read_csv(
            full_path,
            delimiter=' ',
            names=['cadence', 'quality_flag'],
            dtype={'cadence': int, 'quality_flag': int}
        )
        q_df['camera'] = camera
        q_df['ccd'] = ccd
        q_df = q_df.set_index(['cadence', 'camera', 'ccd'])
        q_df.index.rename(['cadence', 'camera', 'ccd'])
        dfs.append(q_df)

    df = pd.concat(dfs)
    df = df[~df.index.duplicated(keep='last')]
    return df

def assign_quality_flags(lp_df, qf_df):
    index = pd.MultiIndex.from_frame(lp_df[['cadence', 'camera', 'ccd']])
    reindexed = qf_df.loc[index]
    lp_df['quality_flag'] = reindexed.quality_flag.values


def align_orbit(lp_df, tmag):
    values = np.array(lp_df['value'])
    quality_flags = np.array(lp_df['quality_flag'])
    good_values = values[quality_flags == 0]
    offset = np.nanmedian(good_values) - tmag
    lp_df['value'] = values - offset


def align_orbits(lp_df, id_tic_map, tic_parameters):
    """
    Performs mass orbit alignment. Requires that lp_df has the correct
    quality flag check.
    """

    for key, lightpoints in lp_df.group_by(['lightcurve_id', 'orbit']):
        lightcurve_id = key[0]
        tic = id_tic_map[lightcurve_id]
        tic_parameters = tic_parameters.loc[tic]

        values = np.array(lp_df['values'])
        mask = lp_df['quality_flags'] == 0
        offset = np.nanmedian(lp_df[mask]) - tic_parameters['tmag']
        lp_df['values'] = values - offset


def align_values(tmag, values, quality_flags):
    good_values = values[quality_flags == 0]
    offset = np.nanmedian(good_values) - tmag
    return values - offset


def approx_lp_mem(lightpoints):
    mem = lightpoints.memory_usage()
    db_columns = [
        'lightcurve_id',
        'cadence',
        'barycentric_julian_date',
        'value',
        'error',
        'x_centroid',
        'y_centroid',
        'quality_flag'
    ]
    total = sum([mem[col] for col in db_columns])
    return total


def yield_new_lc_dict(merged, lc_kwargs):
    merged = merged[merged['lightcurve_id'] < 0]

    mem_requirement = 0
    batch = []
    for id_, lightpoints in merged.groupby('lightcurve_id'):
        sorted_lp = lightpoints.sort_values('cadence')
        r = dict(
            tic_id=lc_kwargs.loc[id_]['tic_id'],
            aperture_id=lc_kwargs.loc[id_]['aperture'],
            lightcurve_type_id=lc_kwargs.loc[id_]['lightcurve_type'],
            cadences=sorted_lp['cadence'],
            barycentric_julian_date=sorted_lp['barycentric_julian_date'],
            values=sorted_lp['value'],
            errors=sorted_lp['error'],
            x_centroids=sorted_lp['x_centroid'],
            y_centroids=sorted_lp['y_centroid'],
            quality_flags=sorted_lp['quality_flag']
        )

        batch.append(r)
        mem_requirement += approx_lp_mem(lightpoints)
        # Check if we've reached the threshold
        if mem_requirement >= THRESHOLD:
            yield batch
            batch = []
    # Clean up any remaining batches
    if len(batch) > 0:
        yield batch


def yield_merge_lc_dict(merged, lc_kwargs):
    merged = merged[merged['lightcurve_id'] > 0]

    mem_requirement = 0
    batch = []
    for id_, lightpoints in merged.groupby('lightcurve_id'):
        sorted_lp = lightpoints.sort_values('cadence')
        r = dict(
            _id=int(id_),
            cadences=sorted_lp['cadence'],
            bjd=sorted_lp['barycentric_julian_date'],
            _values=sorted_lp['value'],
            errors=sorted_lp['error'],
            x_centroids=sorted_lp['x_centroid'],
            y_centroids=sorted_lp['y_centroid'],
            quality_flags=sorted_lp['quality_flag']
        )
        batch.append(r)
        mem_requirement += approx_lp_mem(lightpoints)

        if mem_requirement >= THRESHOLD:
            yield batch
            batch = []
            mem_requirement = 0
    if len(batch) > 0:
        yield batch


def kwargs_to_df(*kwargs, **constants):
    dfs = []
    keys = ['cadences', 'barycentric_julian_date', 'values', 'errors',
            'x_centroids', 'y_centroids', 'quality_flags']

    for kwarg in kwargs:
        df = pd.DataFrame(
            data={
                k: kwarg[k] for k in keys
            }
        )
        df['lightcurve_id'] = kwarg.get('id', None)
        df = df.set_index(['lightcurve_id', 'cadences'])
        dfs.append(df)
    main = pd.concat(dfs)
    for k, constant in constants.items():
        main[k] = constant
    return main

def commit_observations(observation_list, db):
    df = pd.DataFrame(observation_list)
    df = df.set_index(['tic_id', 'orbit_id'])
    df = df[~df.index.duplicated(keep='last')]
    df = df.reset_index()
    db.session.execute(Observation.upsert_dicts(), df.to_dict('records'))


def kwarg_merge(target, *sources):
    args = [
        'barycentric_julian_date',
        'values',
        'errors',
        'x_centroids',
        'y_centroids',
        'quality_flags'
    ]
    cadences = target['cadences']
    kwargs = {
        k: target[k] for k in args
    }

    for source in sources:
        cadences = np.concatenate((cadences, source['cadences']))
        for arg in args:
            kwargs[arg] = np.concatenate(
                (kwargs[arg], source[arg])
            )
    cadences, result = merge_arrays(cadences, **kwargs)
    result['cadences'] = cadences
    return result


def parallel_h5_merge(config, process_id, ingest_qflags, tics):
    # Setup necessary contexts
    job_sqlite = IngestionCache()
    pid = os.getpid()
    observations = []
    jobs = []
    for tic_chunk in chunkify(tics, 999):
        q = job_sqlite.query(IngestionJob).filter(IngestionJob.tic_id.in_(tic_chunk)).all()
        for job in q:
            jobs.append(job)

    orbits = set(job.orbit_number for job in jobs)

    if ingest_qflags:
        quality_flags = load_quality_flags(*orbits)
    logger.debug('Worker-{} parsed file contexts'.format(pid))

    tic_parameters = pd.concat(
        [
            pd.read_sql(
                job_sqlite.query(
                    TIC8Parameters.tic_id,
                    TIC8Parameters.right_ascension.label('ra'),
                    TIC8Parameters.declination.label('dec'),
                    TIC8Parameters.tmag
            ).filter(TIC8Parameters.tic_id.in_(tic_chunk)).statement,
            job_sqlite.bind,
            index_col=['tic_id'])
            for tic_chunk in chunkify(tics, 999)
        ]
    )
    dataframes = []
    tmp_id_map = {}

    for tic_chunk in chunkify(tics, 999):
        id_q = job_sqlite.query(
            LightcurveIDMapper
        ).filter(LightcurveIDMapper.tic_id.in_(tic_chunk))

        for mapper in id_q.all():
            k, v = mapper.to_key_value
            tmp_id_map[k] = v

    job_sqlite.close()
    with db_from_config(
            config,
            executemany_mode='values',
            executemany_values_page_size=10000,
            executemany_batch_page_size=500
        ) as db:

        time_corrector = TimeCorrector(db.session, tic_parameters)
        orbit_map = pd.read_sql(
            db.query(
                Orbit.id.label('orbit_id'),
                Orbit.orbit_number
            ).statement,
            db.session.bind,
            index_col=['orbit_number']
        )

        logger.debug('Worker-{} instantiated TimeCorrector'.format(pid))

        # Preload existing lightcurves into dataframe as lightpoints
        initial_lp = pd.read_sql(
            db.query(
                Lightcurve.id.label('lightcurve_id'),
                func.unnest(Lightcurve.cadences).label('cadence'),
                func.unnest(Lightcurve.bjd).label('barycentric_julian_date'),
                func.unnest(Lightcurve.values).label('value'),
                func.unnest(Lightcurve.errors).label('error'),
                func.unnest(Lightcurve.x_centroids).label('x_centroid'),
                func.unnest(Lightcurve.y_centroids).label('y_centroid'),
                func.unnest(Lightcurve.quality_flags).label('quality_flag')
            ).filter(Lightcurve.tic_id.in_(tics)).statement,
            db.session.bind,
            index_col=['lightcurve_id', 'cadence']
        )
        initial_lp.index.rename(['lightcurve_id', 'cadence'], inplace=True)
        logger.debug('Worker-{} preloaded {} lightpoints'.format(pid, len(initial_lp)))
        dataframes.append(initial_lp)
        #  All needed pretexts are needed to begin ingesting
        tmp_lc_id = -1
        logger.debug('Worker-{} processing files...'.format(pid))
        time_0 = datetime.now()
        for nth, job in enumerate(jobs):
            observation = dict(
                tic_id=job.tic_id,
                camera=job.camera,
                ccd=job.ccd,
                orbit_id=orbit_map.loc[job.orbit_number]['orbit_id']
            )
            observations.append(observation)
            orbit = job.orbit_number
            camera = job.camera
            ccd = job.ccd
            for kwarg in h5_to_kwargs(job.file_path, orbit_number=orbit, camera=camera, ccd=ccd):
                key = (
                    kwarg['tic_id'],
                    kwarg['aperture_id'],
                    kwarg['lightcurve_type_id']
                )
                try:
                    id_ = tmp_id_map[key]
                except KeyError:
                    id_ = tmp_lc_id
                    tmp_id_map[key] = id_
                    tmp_lc_id -= 1

                df = lc_dict_to_df(
                    kwarg,
                    lightcurve_id=id_,
                )

                # Time correct
                earth_tjd = time_corrector.correct(
                    int(observation['tic_id']),
                    time_corrector.mid_tjd(df)
                )
                df['barycentric_julian_date'] = earth_tjd

                # Quality flag assignment
                if ingest_qflags:
                    assign_quality_flags(df, quality_flags)

                # Orbit alignment
                tmag = tic_parameters.loc[kwarg['tic_id']]['tmag']
                align_orbit(df, tmag)

                df = df.set_index(['lightcurve_id', 'cadence'])
                df.index.rename(['lightcurve_id', 'cadence'], inplace=True)
                dataframes.append(df)
            elapsed = datetime.now() - time_0
            if elapsed.total_seconds() > 10:
                logger.debug('Worker-{} status {}/{}'.format(pid, nth, len(jobs)))
                time_0 = datetime.now()

        # Grab lightcurve kwarg dump to properly discern to insert/update
        kwarg_map = pd.DataFrame(
            [dict(tic_id=k[0], aperture=k[1], lightcurve_type=k[2], id=v) for k, v in tmp_id_map.items()]
        )
        kwarg_map.set_index('id', inplace=True)
        # Done! We just need to merge back into the lightcurves table
        # and commit to finalize the changes
        merged = pd.concat(dataframes, sort=False)
        merged = merged[~merged.index.duplicated(keep='last')]
        merged.reset_index(inplace=True)

        for batch in yield_new_lc_dict(merged, kwarg_map):
            logger.debug('Worker-{} inserting {} new lightcurves'.format(pid, len(batch)))
            batch_t0 = datetime.now()
            q = Lightcurve.__table__.insert().values(batch)
            db.session.execute(
                q
            )
            batch_t1 = datetime.now()

            alteration = QLPAlteration(
                process_id=process_id,
                alteration_type='insert',
                target_model='lightcurvedb.models.lightcurve.Lightcurve',
                n_altered_items = len(batch),
                est_item_size = sum([len(x['cadences']) for x in batch]),
                time_start = batch_t0,
                time_end = batch_t1
            )
            db.add(alteration)

        for batch in yield_merge_lc_dict(merged, kwarg_map):
            logger.debug('Worker-{} updating {} lightcurves'.format(pid, len(batch)))
            update_q = Lightcurve.__table__.update().where(
                Lightcurve.id == bindparam('_id')
            ).values({
                Lightcurve.cadences: bindparam('cadences'),
                Lightcurve.bjd: bindparam('bjd'),
                Lightcurve.values: bindparam('_values'),
                Lightcurve.errors: bindparam('errors'),
                Lightcurve.x_centroids: bindparam('x_centroids'),
                Lightcurve.y_centroids: bindparam('y_centroids'),
                Lightcurve.quality_flags: bindparam('quality_flag')
            })
            batch_t0 = datetime.now()
            db.session.execute(update_q, batch)
            batch_t1 = datetime.now()

            alteration = QLPAlteration(
                process_id=process_id,
                alteration_type='update',
                target_model='lightcurvedb.models.lightcurve.Lightcurve',
                n_altered_items = len(batch),
                est_item_size = sum([len(x['cadences']) for x in batch]),
                time_start = batch_t0,
                time_end = batch_t1
            )
            db.add(alteration)

        logger.debug('Worker-{} updating observations'.format(pid))
        observations = pd.DataFrame(observations).set_index(['tic_id', 'orbit_id'])
        observations = observations[~observations.index.duplicated(keep='last')]
        observations.reset_index(inplace=True)

        obs_insert_t0 = datetime.now()
        db.session.execute(
            Observation.upsert_dicts(),
            observations.to_dict('records')
        )
        obs_insert_t1 = datetime.now()

        alteration = QLPAlteration(
            process_id=process_id,
            alteration_type='upsert',
            target_model='lightcurvedb.models.Observation',
            n_altered_items=len(observations),
            est_item_size=4,
            time_start=obs_insert_t0,
            time_end=obs_insert_t1
        )
        db.add(alteration)
        db.commit()
    logger.debug('Worker-{} done'.format(pid))
    return len(tics)


def async_h5_merge(config, tic_queue, lightcurve_queue, time_corrector):
    """
    Merge h5 as fast as possible loading target TICs from the tic_queue.
    Merged lightcurve arguments will be serialized into dictionaries and
    passed into the lightcurve_queue to be inserted into the database.
    This process still needs a connection to the database in order
    to determine past lightcurve data.

    Notes
    -----
    If the lightcurve_queue is full this process will block.
    """
    db = db_from_config(config).open()
    job_sqlite = IngestionCache()
    qflag_df = job_sqlite.quality_flag_df
    pid = os.getpid()
    first_ingestion = True

    id_map = dict()
    cur_tmp_id = -1  # For grouping purposes only in this process

    try:
        while True:
            if not first_ingestion:
                # Timeout after 5 seconds
                tic = tic_queue.get(timeout=5)
            else:
                tic = tic_queue.get(block=True)
                first_ingestion = False

            lightcurves = [
                lc.to_dict
                for lc in db.lightcurves.filter(Lightcurve.tic_id == tic).all()
            ]

            ra, dec, tmag = job_sqlite.query(
                TIC8Parameters.right_ascension,
                TIC8Parameters.declination,
                TIC8Parameters.tmag
            ).get(tic)

            # Load defined lightcurve ids
            for lc in lightcurves:
                key = (tic, lc['aperture_id'], lc['lightcurve_type_id'])
                id_map[key] = lc['id']

            to_ingest = job_sqlite.query(IngestionJob).filter(
                IngestionJob.tic_id == tic
            )
            kwarg_keys = dict()

            # Convert current lightcurves into a single dataframe
            if len(lightcurves) > 0:
                lightpoints = kwargs_to_df(*lightcurves)
            else:
                lightpoints = None

            observations = []

            # Find and merge the relevant files
            for job in to_ingest:
                if ingest_qflags:
                    quality_flags = load_quality_flags(job.orbit_number)

                observation = h5_to_observation(job.file_path)
                observations.append(observation)
                for lc_kwargs in h5_to_kwargs(job.file_path):
                    ap = lc_kwargs['aperture_id']
                    lc_type = lc_kwargs['lightcurve_type_id']
                    lc_kwargs['tic_id'] = tic

                    id_key = (tic, ap, lc_type)
                    _id = id_map.get(id_key, None)
                    lc_kwargs['id'] = _id

                    if _id is None:
                        _id = cur_tmp_id
                        cur_tmp_id -= 1
                        kwarg_keys[_id] = id_key

                    new_lp = kwargs_to_df(
                        lc_kwargs,
                        camera=int(observation['camera']),
                        ccd=int(observation['ccd'])
                    )
                    # Update quality flags
                    update_qflag(qflag_df, new_lp)

                    # Perform orbit align
                    good_values = new_lp[new_lp['quality_flags'] == 0]['values']
                    offset = np.nanmedian(good_values) - tmag
                    new_lp['values'] = new_lp['values'] - offset

                    # Perform time correction
                    corrected_times = time_corrector.correct_bjd(
                        ra, dec, new_lp
                    )
                    new_lp['barycentric_julian_date'] = corrected_times

                    # Concat to lightpoint df
                    if lightpoints is not None:
                        lightpoints = pd.concat((lightpoints, new_lp), sort=False)
                    else:
                        lightpoints = new_lp
            # Ingested all jobs, perform merge
            lightpoints = lightpoints[~lightpoints.index.duplicated(keep='last')]
            lightpoints.reset_index(inplace=True)

            # Group by id, sort by cadence, and send merged lightcurve to queue
            for _id, lp in lightpoints.groupby('lightcurve_id'):
                sorted_lp = lp.sort_values('cadences')
                data = dict(
                    cadences=list(sorted_lp['cadences']),
                    bjd=list(sorted_lp['barycentric_julian_date']),
                    values=list(sorted_lp['values']),
                    errors=list(sorted_lp['errors']),
                    x_centroids=list(sorted_lp['x_centroids']),
                    y_centroids=list(sorted_lp['y_centroids']),
                    quality_flags=list(sorted_lp['quality_flags']),
                    observations=observations
                )

                if _id < 0:
                    # Just need to insert, send needed data
                    tic_id, ap_id, lc_t_id = kwarg_keys[_id]
                    data['tic_id'] = tic_id
                    data['aperture_id'] = ap_id
                    data['lightcurve_type_id'] = lc_t_id
                    data['_id'] = None
                else:
                    # Just need to provide id for update
                    data['_id'] = _id

                lightcurve_queue.put(data)
            tic_queue.task_done()
    except queue.Empty:
        logger.debug('Worker-{} timed-out getting job, exiting')
    finally:
        # Cleanup connections
        job_sqlite.close()
        db.close()
        logger.debug('Worker-{} exiting')
