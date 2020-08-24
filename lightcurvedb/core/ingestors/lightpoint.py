try:
    # Python agnostic import of Queue
    import queue
except ImportError:
    import Queue as queue

import h5py
import numpy as np
import pandas as pd
import os
from collections import namedtuple
from lightcurvedb.models import Lightcurve, Lightpoint, Orbit, Observation
from lightcurvedb.legacy.timecorrect import StaticTimeCorrector
from lightcurvedb.core.ingestors.lightcurve_ingestors import h5_to_kwargs, kwargs_to_df
from lightcurvedb.core.ingestors.quality_flag_ingestors import update_qflag
from lightcurvedb.core.ingestors.temp_table import QualityFlags
from lightcurvedb.util.logger import lcdb_logger as logger
from lightcurvedb.util.iter import chunkify
from lightcurvedb import db_from_config
from sqlalchemy import Integer, text, bindparam
from sqlalchemy.sql import select, func, cast
from multiprocessing import Process

try:
    from math import isclose
except ImportError:
    # Python 2
    def isclose(x, y):
        return np.isclose([x], [y])[0]

LP_COLS = ['lightcurve_id', 'cadence', 'barycentric_julian_date',
        'data', 'error', 'x_centroid', 'y_centroid', 'quality_flag'
        ]

LP_COL_RENAME = dict(
    cadences='cadence',
    values='data',
    errors='error',
    x_centroids='x_centroid',
    y_centroids='y_centroid',
    quality_flags='quality_flag'
)

DIFF_COLS = [
    'values',
    'barycentric_julian_date',
    'quality_flags'
]

MergeJob = namedtuple(
    'MergeJob',
    ('tic_id', 'ra', 'dec', 'tmag',
    'file_observations',
    'cur_id_map')
)


def redundant(current_lp, database_lp):

    cmpr = current_lp.join(
        database_lp,
        how='inner',
        lsuffix='_current',
        rsuffix='_database'
    )

    for diff_col in DIFF_COLS:
        # short circut loop checking if values are redundant
        current_col = cmpr['{}_current'.format(diff_col)]
        database_col = cmpr['{}_database'.format(diff_col)]
        diff = current_col - database_col

        diff_sum = sum(diff)
        if not isclose(diff_sum, 0.0):
            return False, None

    return True, cmpr.index



def lightpoint_upsert_q(
        lightcurve_id='lightcurve_id',
        cadence='cadence',
        bjd='barycentric_julian_date',
        data='data',
        error='error',
        x='x_centroid',
        y='y_centroid',
        quality_flag='quality_flag',
        mode='overwrite'):
    """
    Create an insertion sql expression that takes a certain action
    on collision with the primary key.

    Parameters
    ----------
    mode: str
        The name of the behavior to use. Currently accepted values are
        'overwrite' and 'nothing'.
    Raises
    ------
    ValueError
        Raised if given an unknown mode.
    """

    mapping = {
        getattr(Lightpoint, 'lightcurve_id'): bindparam(lightcurve_id),
        getattr(Lightpoint, 'cadence'): bindparam(cadence),
        getattr(Lightpoint, 'bjd'): bindparam(bjd),
        getattr(Lightpoint, 'data'): bindparam(data),
        getattr(Lightpoint, 'error'): bindparam(error),
        getattr(Lightpoint, 'x'): bindparam(x),
        getattr(Lightpoint, 'y'): bindparam(y),
        getattr(Lightpoint, 'quality_flag'): bindparam(quality_flag)
    }

    q = Lightpoint.insert().values(mapping)
    constraint = 'lightpoints_pkey'

    if mode == 'overwrite':
        q = q.on_conflict_do_update(
            constraint=constraint,
            set_=dict(
                barycentric_julian_date=q.excluded.barycentric_julian_date,
                data=q.excluded.data,
                error=q.excluded.error,
                x_centroid=q.excluded.x_centroid,
                y_centroid=q.excluded.y_centroid,
                quality_flag=q.excluded.quality_flag
            )
        )
    elif mode == 'nothing':
        q = q.on_conflict_do_nothing(
            constraint=constraint
        )
    elif mode is None:
        return q
    else:
        raise ValueError('Unknown mode {}'.format(mode))

    return q


class LightpointProcessor(Process):
    def log(self, msg, level='debug'):
        getattr(logger, level)('{}: {}'.format(self.name, msg))

    def set_name(self):
        self.name = '{}-{}'.format(self.prefix, os.getpid())


class LightpointInserter(LightpointProcessor):
    prefix = 'Inserter'
    def __init__(self, lcdb_config, insertion_queue, resolution_mode, **process_kwargs):
        super(LightpointInserter, self).__init__(**process_kwargs)
        self.engine_kwargs = dict(
            executemany_mode='values',
            executemany_values_page_size=10000,
            executemany_batch_page_size=500
        )
        self.queue = insertion_queue
        self.config = lcdb_config
        self.resolution_mode = resolution_mode
        self.db = None

        self.lc_cache = []
        self.lp_cache = None

        self.orbit_map = {}
        self.observation_cache = []

        self.id_map_cache = []

    def resolve_lcs(self, lightcurve_ids):
        if self.resolution_mode == 'ignore':
            # Drop lightpoints from the lightpoint cache
            idx = self.lp_cache.loc[list(lightcurve_ids)].index
            self.lp_cache.drop(idx, inplace=True)
        else:
            self.log('consolidating {} lightcurves from db'.format(
                len(lightcurve_ids)
            ))
            q = self.db.query(
                Lightpoint.lightcurve_id,
                func.array_agg(Lightpoint.cadence)
            ).filter(
                Lightpoint.lightcurve_id.in_(lightcurve_ids)
            ).group_by(Lightpoint.lightcurve_id)

            colliding_ids = []
            colliding_lps = []

            for id_, cadences in q.all():
                try:
                    new_cadences = set(
                        self.lp_cache.loc[id_].index.values
                    )
                except KeyError:
                    self.log('Could not find {} in {}'.format(
                        id_,
                        set(self.lp_cache.index.get_level_values('lightcurve_id'))
                    ))
                    raise
                colliding = set(cadences) & new_cadences
                if len(colliding) > 0:
                    # We need to update this lightcurve
                    colliding_ids.append(id_)

            self.log('Found colliding IDs {}'.format(colliding_ids))

            colliding_lps = Lightpoint.get_as_df(colliding_ids, self.db)

            if len(colliding_lps) == 0:
                # Got an empty lightcurve, we can go ahead and just
                # insert
                return

            # Check for redundancy, if redundant, drop redundant values
            is_redundant, redundant_idx = redundant(self.lp_cache, colliding_lps)
            if is_redundant:
                self.log('found {} redundant lps, dropping'.format(len(redundant_idx)))
                self.lp_cache.drop(
                    index=redundant_idx,
                    inplace=True
                )
                return

            if len(colliding_lps) > 0:
                # We need to replace all relevant ids
                self.db.query(Lightpoint).filter(
                    Lightpoint.lightcurve_id.in_(colliding_ids)
                ).delete(synchronize_session=False)

                merged = pd.concat((
                    colliding_lps,
                    self.lp_cache
                ), sort=True)
                merged = merged[~merged.index.duplicated(keep='last')]
                self.lp_cache = merged

    @property
    def cache_threshold(self):
        if self.lp_cache is None:
            return False

        if len(self.lp_cache) >= 10**6:
            return True
        return False

    def flush(self):
        """
        Flush lightpoint cache to disk
        """
        if len(self.lc_cache) < 0 or self.lp_cache is None:
            self.log(
                'prematurely exiting flush, no valid data to insert!'
            )
            return
        db = self.db

        # Prepare and merge lp cache
        self.prepare_lp_cache()
        
        db.session.add_all(self.lc_cache)
        db.session.commit()

        self.lp_cache.sort_index(inplace=True)
        self.lp_cache = self.lp_cache[~self.lp_cache.index.duplicated(keep='last')]
        self.log('inserting {} lightpoints'.format(len(self.lp_cache)))
        self.lp_cache.reset_index(inplace=True)
        self.lp_cache.rename(
            columns=LP_COL_RENAME,
            inplace=True
        )

        lp = self.lp_cache[LP_COLS]
        q = lightpoint_upsert_q(mode='overwrite')
        for chunk in chunkify(lp.to_dict('records'), 10000):
            db.session.execute(q, chunk)
        
        q = Observation.upsert_dicts()
        obs = pd.DataFrame(self.observation_cache)
        self.log(obs)
        obs.set_index('tic_id', 'orbit_id', inplace=True)
        obs.sort_index(inplace=True)
        obs = obs[~obs.index.duplicated(keep='last')]
        obs.reset_index(inplace=True)
        db.session.execute(
            q,
            obs.to_dict('records')
        )
        db.commit()

        self.lc_cache = []
        self.lp_cache = None
        self.observation_cache = []
        self.id_map_cache = []

    def map_observations(self, observations):
        results = []
        for obs in observations:
            results.append(
                dict(
                    tic_id=obs['tic_id'],
                    camera=obs['camera'],
                    ccd=obs['ccd'],
                    orbit_id=self.orbit_map[obs['orbit']]
                )
            )
        return results

    def prepare_lp_cache(self):
        # Collapse mappers to a single dictionary
        tmp_id_mapper = {}
        for mapper in self.id_map_cache:
            tmp_id_mapper.update(mapper)
        new_lcs, ids_to_update, id_mapper = self.resolve_tmp_ids(tmp_id_mapper)
        lightcurves = [Lightcurve(**kw) for kw in new_lcs]

        self.lc_cache.extend(lightcurves)

        lp = self.lp_cache
        lp.reset_index(inplace=True)
        lp['lightcurve_id'] = lp['lightcurve_id'].apply(
            lambda tmp_id: id_mapper.get(tmp_id, tmp_id)
        )

        lp.set_index(['lightcurve_id', 'cadences'], inplace=True)
        # Resolve and merge known lightcurves with current data
        #self.resolve_lcs(ids_to_update)

    def insert(self, job):
        observations, tmp_id_mapper, lp = job
        observations = self.map_observations(observations)
        self.observation_cache.extend(observations)
        self.id_map_cache.append(tmp_id_mapper)

        if self.lp_cache is None:
            self.lp_cache = lp
        else:
            self.lp_cache = pd.concat((self.lp_cache, lp))

        self.flush()

    def resolve_tmp_ids(self, mapper):
        to_resolve = {k for k, v in mapper.items() if v < 0}

        n_to_resolve = len(to_resolve)

        if n_to_resolve > 0:
            cmd = text(
                'SELECT NEXTVAL(:seq) FROM generate_series(1,:length)'
            ).bindparams(seq='lightcurves_id_seq', length=n_to_resolve)

            ids = {
                r for r, in
                self.db.session.execute(cmd).fetchall()
            }
        else:
            ids = {}

        tmp_id_to_real = dict()
        new_lc_kws = []
        lcs_to_update = []

        for k, id_ in mapper.items():
            if k in to_resolve:
                tmp_id_to_real[id_] = ids.pop()
                new_lc_kws.append(
                    dict(
                        id=tmp_id_to_real[id_],
                        tic_id=k[0],
                        aperture_id=k[1],
                        lightcurve_type_id=k[2]
                    )
                )
            else:
                tmp_id_to_real[k] = id_
                lcs_to_update.append(id_)
        return new_lc_kws, lcs_to_update, tmp_id_to_real


    def run(self):
        self.set_name()
        self.log('running')
        self.db = db_from_config(self.config, **self.engine_kwargs).open()
        self.orbit_map = {
            o.orbit_number: o.id for o in self.db.query(Orbit).all()
        }
        try:
            first_ingestion = True
            while True:
                if first_ingestion:
                    job = self.queue.get()
                    first_ingestion = False
                else:
                    job = self.queue.get(timeout=20)
                self.log('was given {} lightpoints'.format(len(job[2])))
                self.insert(job)
                self.queue.task_done()
        except queue.Empty:
            self.log('queue timeout, flushing cache', level='info')
            self.flush()
        except KeyboardInterrupt:
            self.log('keyboard interrupt, flushing cache', level='info')
            self.flush()
        except EOFError as e:
            self.log('unexpected EOF: {}'.format(e))
        except Exception:
            logger.error('encountered critical error')
            logger.exception('{} encountered an exception')
        finally:
            self.log('exited', level='debug')
            self.db.close()


class LightpointH5Merger(LightpointProcessor):
    prefix = 'Merger'
    def __init__(
            self,
            merge_queue,
            insertion_queue,
            time_corrector,
            quality_flags,
            **process_kwargs):
        super(LightpointH5Merger, self).__init__(**process_kwargs)

        # Grab needed contextss
        self.merge_queue = merge_queue
        self.insertion_queue = insertion_queue

        self.q_flags = quality_flags
        self.set_name()
        self.time_corrector = time_corrector
        self.cur_tmp_id = -1

    def get_id(self, tic, aperture, lc_type, id_map):
        """
        Resolve an ID from the given lightcurve keyword arguments.
        IDs returned are reserved in this context:

        id > 0: Reserved for lightcurves that currently exist within the
        database. The existing data will be merged in the actual insertion
        processes that have maintained database connections.

        id == 0 or id is None: Reserved for errors/unknown data

        id < 0: New lightcurve. Insertion processes will need to create
        new lightcurve instances before mass lightpoint insertion.
        """
        key = (
            tic,
            aperture,
            lc_type
        )
        try:
            return id_map[key]
        except KeyError:
            id_map[key] = self.cur_tmp_id
            self.cur_tmp_id -= 1
            return id_map[key]

    def merge(self, job):
        bundled_lps = []
        cur_id_map = job.cur_id_map

        observations = []

        for obs in job.file_observations:
            tic, orbit, camera, ccd, file_path = obs
            observations.append(dict(
                tic_id=tic,
                orbit=orbit,
                camera=camera,
                ccd=ccd
            ))
            for kw in h5_to_kwargs(file_path):
                lc_id = self.get_id(tic, kw['aperture_id'], kw['lightcurve_type_id'], cur_id_map)
                kw['id'] = lc_id
                h5_lp = kwargs_to_df(
                    kw,
                    camera=camera,
                    ccd=ccd,
                    orbit=orbit
                )

                # Update quality flags
                #update_qflag(self.q_flags, h5_lp)
                h5_by_physical = h5_lp.reset_index().set_index(['cadences', 'camera', 'ccd'])
                joined = h5_by_physical.join(
                    self.q_flags,
                    rsuffix='_ingested',
                    lsuffix='_correct'
                )
                joined.reset_index(inplace=True)
                joined = joined.set_index(['lightcurve_id', 'cadences'])
                h5_lp.loc[joined.index, 'quality_flags'] = joined['quality_flags_correct']

                # Align data
                good_values = h5_lp.loc[h5_lp['quality_flags'] == 0, 'values']
                offset = np.nanmedian(good_values) - job.tmag
                h5_lp['values'] = h5_lp['values'] - offset

                # Timecorrect
                corrected_bjd = self.time_corrector.correct_bjd(
                    job.ra,
                    job.dec,
                    h5_lp
                )
                h5_lp['barycentric_julian_date'] = corrected_bjd

                # Orbital data has been corrected for Earth observation
                # and reference
                # Remove unneeded bookkeeping data. No need to duplicate it
                # at crosses Process memory contexts.
                h5_lp.drop(['orbit', 'camera', 'ccd'], inplace=True, axis=1)
                bundled_lps.append(h5_lp)

        merged = pd.concat(bundled_lps)

        total_ids = len(cur_id_map)
        tmp_ids = len({v for k, v in cur_id_map.items() if v < 0})
        real_ids = total_ids - tmp_ids

        self.log('parsed {} with {} files for {} lightpoints with {} TMP ids and {} real ids'.format(
            job.tic_id, len(job.file_observations),
            len(merged), tmp_ids, real_ids
        ))

        return observations, cur_id_map, merged

    def run(self):
        self.set_name()
        self.log('initialized', level='debug')
        first_job = True
        try:
            while True:
                if first_job:
                    job = self.merge_queue.get()
                    first_job = False
                else:
                    job = self.merge_queue.get(timeout=10)
                _, id_map, lp = self.merge(job)
                observations = [
                    {
                        'tic_id': f[0],
                        'orbit': f[1],
                        'camera': f[2],
                        'ccd': f[3]
                    }
                    for f in job.file_observations
                ]
                # Lightpoints have been merged across the tic, send to
                # insertion queue for processing. For now build the needed
                # context for proper ingestion
                insert_job = (observations, id_map, lp)
                self.insertion_queue.put(insert_job, timeout=10*60)
        except queue.Empty:
            self.log('queue timed out, exiting')
        except KeyboardInterrupt:
            self.log('received interrupt...closing', level='info')
            return
        finally:
            self.log('exiting')
