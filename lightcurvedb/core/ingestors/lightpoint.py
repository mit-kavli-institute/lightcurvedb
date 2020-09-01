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
from lightcurvedb.core.datastructures.data_packers import DataPacker
from lightcurvedb.util.logger import lcdb_logger as logger
from lightcurvedb.util.iter import chunkify
from lightcurvedb import db_from_config
from sqlalchemy import Integer, text, bindparam, Sequence
from sqlalchemy.orm import joinedload
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
    'file_observations')
)


def cadence_map_to_iter(id_cadence_map):
    for id_, cadences in id_cadence_map.items():
        for cadence in cadences:
            yield id_, cadence


def remove_redundant(id_cadence_map, current_lp):
    """
    Drop any (lightcurve_id, cadence) pairs that appear in the database
    to avoid duplication.
    """
    return current_lp.drop(
        cadence_map_to_iter(id_cadence_map),
        errors='ignore'
    )


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


class MassIngestor(LightpointProcessor):
    prefix = 'MassIngestor'

    def __init__(
            self,
            lcdb_config,
            quality_flags,
            time_corrector,
            tic_queue,
            scratch_dir,
            mode='ignore',
            **process_kwargs):
        super(MassIngestor, self).__init__(**process_kwargs)
        self.engine_kwargs = dict(
            executemany_mode='values',
            executemany_values_page_size=10000,
            executemany_batch_page_size=500
        )
        self.sequence = Sequence('lightcurves_id_seq')
        self.config = lcdb_config
        self.tic_queue = tic_queue
        self.mode = mode
        self.q_flags = quality_flags
        self.time_corrector = time_corrector

        self.db = None
        self.cadence_to_orbit_map = dict()
        self.orbit_map = dict()
        self.observations = []
        self.packer = DataPacker(scratch_dir)

        self.new_ids = set()
        self.id_map = dict()

    def cadence_map(self, tic):
        q = db.query(
            Lightcurve.id.label('lightcurve_id'),
            func.array_agg(Lightpoint.cadence).label('cadences')
        ).join(Lightpoint.lightcurve).filter(
            Lightcurve.tic_id == tic
        ).group_by(Lightcurve.id)

        return {
            id_: cadences for id_, cadences in q.all()
        }

    def update_ids(self, tic):
        for lc in db.lightcurves.filter(Lightcurve.tic_id == tic).all():
            key = (lc.tic_id, lc.aperture_id, lc.lightcurve_type_id)
            self.id_map[key] = lc.id

    def get_id(self, tic, aperture, lc_type):
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
            return self.id_map[key]
        except KeyError:
            self.id_map[key] = self.db.execute(
                self.sequence
            )
            self.new_ids.add(id_map[key])
            return self.id_map[key]

    def merge(self, job):
        bundled_lps = []
        observations = []
        self.update_ids(job.tic_id)
        cadence_map = self.cadence_map(job.tic_id)
        observed_orbits = {
            r for r, in self.db.query(
                Orbit.orbit_number
            ).join(Observation.orbit).filter(
                Observation.tic_id == job.tic_id
            ).all()
        }

        for obs in job.file_observations:
            tic, orbit, camera, ccd, file_path = obs
            if orbit in observed_orbits:
                self.log('Found duplicate orbit {}'.format(orbit))
                continue
            self.observation_cache.append(dict(
                tic_id=tic,
                orbit_id=self.orbit_map[orbit],
                camera=camera,
                ccd=ccd
            ))
            for kw in h5_to_kwargs(file_path):
                lc_id = self.get_id(
                    tic,
                    kw['aperture_id'],
                    kw['lightcurve_type_id']
                )
                kw['id'] = lc_id
                h5_lp = kwargs_to_df(
                    kw,
                    camera=camera,
                    ccd=ccd,
                    orbit=orbit
                )

                h5_lp = remove_redundant(cadence_map, h5_lp)

                # Update quality flags
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

                # Send to datapacker
                self.packer.pack(h5_lp)
        self.log(
            'processed {} with {} orbits'.format(
                job.tic_id, len(job.file_observations)
            )
        )


    def flush(self):
        """Flush all caches to database"""
        # Insert all new lightcurves
        self.log('Flushing to database...')
        lcs = []
        observations = []
        for key, id_ in self.id_map.items():
            tic_id, ap_id, lc_type_id = key
            lc = Lightcurve(
                id=id_,
                tic_id=tic_id,
                aperture_id=ap_id,
                lightcurve_type_id=lc_type_id
            )
            lcs.append(lc)
        for obs_kw in self.observations:
            observations.append(
                Observation(**obs_kw)
            )
        # Insert new required info first
        self.db.session.add_all(lcs)
        self.db.session.add_all(observations)
        self.db.commit()
        self.log('Sending {} lightpoints'.format(len(self.packer)), level='info')
        self.packer.serialize_to_database(self.db)
        self.db.commit()

        # Reset
        self.id_map = dict()
        self.new_ids = set()
        self.observations = []
        self.packer.close()
        self.packer.open()
        self.log('Flushed')

    def run(self):
        self.db = db_from_config(self.config, **self.engine_kwargs).open()
        self.set_name()
        self.packer.open()

        self.orbit_map = {
            orbit_number: orbit_id for orbit_number, orbit_id in
            self.db.query(Orbit.orbit_number, Orbit.id).all()
        }

        try:
            while True:
                job = self.tic_queue.get(timeout=30)
                self.merge(job)
                self.tic_queue.task_done()

                if len(self.id_map) > 1000:
                    # Flush
                    self.flush()

        except queue.Empty:
            # Timed out :(
            self.log('TIC queue timed out. Flushing any remaining data')
            if len(self.id_map) > 0:
                self.flush()
        except KeyboardInterrupt:
            self.log('Received interrupt signal, flushing before exiting...')
            self.flush()
        finally:
            # Cleanup!
            self.db.close()


class LightpointInserter(LightpointProcessor):
    prefix = 'Inserter'
    def __init__(self, lcdb_config, insertion_queue, resolution_mode, mode='nothing', **process_kwargs):
        super(LightpointInserter, self).__init__(**process_kwargs)
        self.engine_kwargs = dict(
            executemany_mode='values',
            executemany_values_page_size=10000,
            executemany_batch_page_size=500
        )
        self.mode = mode
        self.queue = insertion_queue
        self.config = lcdb_config
        self.resolution_mode = resolution_mode
        self.db = None

        self.lc_cache = []
        self.lp_cache = None

        self.orbit_map = {}
        self.observation_cache = []

        self.id_map_cache = []

    @property
    def cache_threshold(self):
        if self.lp_cache is None:
            return False

        if len(self.lp_cache) >= 10**9:
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
        q = lightpoint_upsert_q(mode=self.mode)
        for chunk in chunkify(lp.to_dict('records'), 10000):
            db.session.execute(q, chunk)
        
        q = Observation.upsert_dicts()
        obs = pd.DataFrame(self.observation_cache)
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

    def insert(self, job):
        observations, tmp_id_mapper, lp = job
        observations = self.map_observations(observations)
        self.observation_cache.extend(observations)
        self.id_map_cache.append(tmp_id_mapper)

        if self.lp_cache is None:
            self.lp_cache = lp
        else:
            self.lp_cache = pd.concat((self.lp_cache, lp))

        if self.cache_threshold:
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
