try:
    # Python agnostic import of Queue
    import queue
except ImportError:
    import Queue as queue

import numpy as np
import os
from collections import namedtuple, defaultdict
from time import time
from lightcurvedb.models import Lightcurve, Lightpoint, Orbit, Observation
from lightcurvedb.core.ingestors.lightcurve_ingestors import (
        h5_to_kwargs, kwargs_to_df
)
from lightcurvedb.core.base_model import QLPModel
from lightcurvedb.util.logger import lcdb_logger as logger
from lightcurvedb import db_from_config
from sqlalchemy import bindparam, Sequence, Table
from multiprocessing import Process
from pgcopy import CopyManager

try:
    from math import isclose
except ImportError:
    # Python 2
    def isclose(x, y):
        return np.isclose([x], [y])[0]

LP_COLS = [
    'lightcurve_id',
    'cadence',
    'barycentric_julian_date',
    'data',
    'error',
    'x_centroid',
    'y_centroid',
    'quality_flag'
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
    (
        'tic_id',
        'ra',
        'dec',
        'tmag',
        'file_observations'
    )
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


class LightpointProcessor(Process):
    def log(self, msg, level='debug'):
        getattr(logger, level)('{0}: {1}'.format(self.name, msg))

    def set_name(self):
        self.name = '{0}-{1}'.format(self.prefix, os.getpid())


class MassIngestor(LightpointProcessor):
    prefix = 'MassIngestor'

    def __init__(
            self,
            lcdb_config,
            quality_flags,
            time_corrector,
            tic_queue,
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
        self.cadence_to_orbit_map = {}
        self.orbit_map = {}
        self.observations = []
        self.lp_cache = defaultdict(list)
        self.table_cache = {}

        self.new_ids = set()
        self.id_map = {}

    def update_ids(self, tic):
        for lc in self.db.lightcurves.filter(Lightcurve.tic_id == tic).all():
            key = (lc.tic_id, lc.aperture_id, lc.lightcurve_type_id)
            self.id_map[key] = lc.id

    def get_id(self, tic, aperture, lc_type):
        key = (
            tic,
            aperture,
            lc_type
        )
        try:
            return self.id_map[key]
        except KeyError:
            self.id_map[key] = self.db.session.execute(
                self.sequence
            )
            self.new_ids.add(self.id_map[key])
            return self.id_map[key]

    def merge(self, job):
        self.update_ids(job.tic_id)
        observed_orbits = {
            r for r, in self.db.query(
                Orbit.orbit_number
            ).join(Observation.orbit).filter(
                Observation.tic_id == job.tic_id
            ).all()
        }
        processed_orbits = set()

        total_len = 0
        for obs in job.file_observations:
            tic, orbit, camera, ccd, file_path = obs
            if orbit in observed_orbits or orbit in processed_orbits:
                self.log('Found duplicate orbit {0}'.format(orbit))
                continue
            self.observations.append(dict(
                tic_id=tic,
                orbit_id=self.orbit_map[orbit],
                camera=camera,
                ccd=ccd
            ))
            processed_orbits.add(orbit)
            for kw in h5_to_kwargs(file_path):
                lc_id = self.get_id(
                    tic,
                    kw['aperture_id'],
                    kw['lightcurve_type_id']
                )
                kw['id'] = lc_id

                # Update quality flags
                idx = [(cadence, camera, ccd) for cadence in kw['cadences']]
                updated_qflag = self.q_flags.loc[idx]['quality_flags']
                updated_qflag = updated_qflag.to_numpy()
                kw['quality_flags'] = updated_qflag

                h5_lp = kwargs_to_df(
                    kw,
                    camera=camera
                )

                # Align data
                mask = h5_lp['quality_flags'] == 0
                good_values = h5_lp.loc[mask]['values'].to_numpy()
                offset = np.nanmedian(good_values) - job.tmag
                h5_lp['values'] = h5_lp['values'] - offset

                # Timecorrect
                corrected_bjd = self.time_corrector.correct_bjd(
                    job.ra,
                    job.dec,
                    h5_lp
                )
                h5_lp['barycentric_julian_date'] = corrected_bjd
                h5_lp.drop(columns=['camera'], inplace=True)
                h5_lp.sort_index(inplace=True)
                h5_lp.reset_index(inplace=True)

                # Orbital data has been corrected for Earth observation
                # and reference
                # Send to the appropriate table
                partition_begin = (lc_id // 1000) * 1000
                partition_end = partition_begin + 1000
                table = 'lightpoints_{0}_{1}'.format(
                    partition_begin,
                    partition_end
                )

                self.lp_cache[table].append(h5_lp)

                total_len += len(h5_lp)

        self.log(
            'processed {0} with {1} orbits for {2} new lightpoints'.format(
                job.tic_id, len(job.file_observations), total_len
            )
        )

    def flush(self):
        """Flush all caches to database"""
        # Insert all new lightcurves
        lcs = []
        for key, id_ in self.id_map.items():
            if id_ not in self.new_ids:
                continue
            tic_id, ap_id, lc_type_id = key
            lcs.append(Lightcurve(
                id=id_,
                tic_id=tic_id,
                aperture_id=ap_id,
                lightcurve_type_id=lc_type_id
            ))

        self.db.session.add_all(lcs)
        self.db.commit()
        points = 0
        for table, lps in self.lp_cache.items():
            try:
                partition = self.table_cache[table]
            except KeyError:
                partition = Table(
                    table,
                    QLPModel.metadata,
                    schema='partitions',
                    autoload=True,
                    autoload_with=self.db.session.bind
                )
                self.table_cache[table] = partition

            q = partition.insert().values({
                Lightpoint.lightcurve_id: bindparam('_id'),
                Lightpoint.bjd: bindparam('bjd'),
                Lightpoint.cadence: bindparam('cadences'),
                Lightpoint.data: bindparam('values'),
                Lightpoint.error: bindparam('errors'),
                Lightpoint.x_centroid: bindparam('x_centroids'),
                Lightpoint.y_centroid: bindparam('y_centroids'),
                Lightpoint.quality_flag: bindparam('quality_flags'),
            })
            for lp in lps:
                df = lp.reset_index().rename(
                    columns={
                        'lightcurve_id': '_id',
                        'barycentric_julian_date': 'bjd'
                    }
                )
                self.db.session.execute(
                    q,
                    df.to_dict('records')
                )
                points += len(df)

        obs_objs = []
        for obs in self.observations:
            o = Observation(**obs)
            obs_objs.append(o)

        self.db.session.add_all(obs_objs)
        self.db.commit()

        # Reset
        self.new_ids = set()
        self.observations = []
        self.lp_cache = defaultdict(list)
        self.log('flushed {0} lightpoints'.format(points))

    def run(self):
        self.db = db_from_config(self.config, **self.engine_kwargs).open()
        self.set_name()

        self.orbit_map = {
            orbit_number: orbit_id for orbit_number, orbit_id in
            self.db.query(Orbit.orbit_number, Orbit.id).all()
        }
        first_ingestion = True

        try:
            while True:
                if first_ingestion:
                    job = self.tic_queue.get()
                    first_ingestion = False
                else:
                    job = self.tic_queue.get(timeout=30)
                self.merge(job)
                self.tic_queue.task_done()
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


class CopyProcess(LightpointProcessor):
    prefix = 'Copier'

    def __init__(self, db_config, lp_queue, copy_args, **process_kwargs):
        super(CopyProcess, self).__init__(**process_kwargs)
        self.lp_queue = lp_queue
        self.threshold_time = copy_args['threshold_time']
        self.expected_tics = copy_args['expected_tics']
        self.db = None
        self.config = db_config

        self.lightcurve_cache = []
        self.internal_cache = []
        self.observation_cache = []
        self.last_copy_time = time()
        self.mgr = None
        self.dirty = False
        self.seen_tics = set()
        self.track_count = 0
        self.pg_map = None

        self.engine_kwargs = dict(
            executemany_mode='values',
            executemany_values_page_size=10000,
            executemany_batch_page_size=500
        )

    def process(self, job):
        lightcurves = job['lightcurves']
        records = job['lightpoints']
        observations = job['observations']
        self.lightcurve_cache.extend(lightcurves)
        self.internal_cache.append(records)
        self.observation_cache.extend(observations)

        if any(len(x) > 0 for x in [lightcurves, records, observations]):
            self.dirty = True

    def iter_lps(self):
        for df in self.internal_cache:
            for row in df.to_records():
                yield row
                self.track_count += 1

    def flush(self):
        if self.dirty:
            lcs = []
            for id_, tic_id, ap_id, lc_type_id in self.lightcurve_cache:
                lc = Lightcurve(
                    id=id_,
                    tic_id=tic_id,
                    aperture_id=ap_id,
                    lightcurve_type_id=lc_type_id
                )
                lcs.append(lc)
                self.seen_tics.add(lc.tic_id)

            self.db.session.add_all(lcs)
            self.db.commit()
            mgr = CopyManager(
                self.db.session.connection().connection,
                'lightpoints',
                ['lightcurve_id', 'cadence', 'barycentric_julian_date',
                    'data', 'error', 'x_centroid', 'y_centroid',
                    'quality_flag']
            )
            mgr.threading_copy(self.iter_lps())
            obs = [Observation(**kw) for kw in self.observation_cache]
            self.db.session.add_all(obs)
            self.db.commit()
            self.last_copy_time = time()
            self.dirty = False

            self.log(
                'Committed {0} lightpoints. Seen ~ {1} / {2} tics'.format(
                    self.track_count,
                    len(self.seen_tics),
                    len(self.expected_tics)
                )
            )
            self.track_count = 0
            self.lightpoint_cache = []
            self.lightcurve_cache = []
            self.observation_cache = []

    def run(self):
        self.db = db_from_config(self.config, **self.engine_kwargs).open()
        self.set_name()

        try:
            while True:
                job = self.lp_queue.get()
                self.process(job)
                self.flush()
        except queue.Empty:
            # Looks like we're done.
            self.log('Finishing up')
            self.flush()
            self.db.close()
        except KeyboardInterrupt:
            self.log('Received interrupt')
            self.flush()
            self.db.close()
