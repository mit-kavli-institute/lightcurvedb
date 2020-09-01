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
        self.packer = DataPacker(
            scratch_dir,
            pack_options={
                'header': False,
                'index': False,
                'na_rep': 'NaN'
            }
        )

        self.new_ids = set()
        self.id_map = dict()

    def cadence_map(self, tic):
        q = self.db.query(
            Lightcurve.id.label('lightcurve_id'),
            func.array_agg(Lightpoint.cadence).label('cadences')
        ).join(Lightpoint.lightcurve).filter(
            Lightcurve.tic_id == tic
        ).group_by(Lightcurve.id)

        return {
            id_: cadences for id_, cadences in q.all()
        }

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

        total_len = 0
        for obs in job.file_observations:
            tic, orbit, camera, ccd, file_path = obs
            if orbit in observed_orbits:
                self.log('Found duplicate orbit {}'.format(orbit))
                continue
            self.observations.append(dict(
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
                if len(h5_lp) == 0:
                    # Nothing to do!
                    # Remove from map
                    key = tic, kw['aperture_id'], kw['lightcurve_type_id']
                    id_ = self.id_map[key]
                    self.log(
                        '{} orbit: {} already ingested'.format(
                            id_, orbit
                        )
                    )
                    continue

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
                # standardize and send to packer
                lp_mapper = dict(
                    cadences='cadence',
                    values='data',
                    errors='error',
                    x_centroids='x_centroid',
                    y_centroids='y_centroid',
                    quality_flags='quality_flag'
                )
                self.packer.pack(h5_lp.reset_index().rename(columns=lp_mapper))
                total_len += len(h5_lp)

        if total_len == 0:
            self.flush_observations()
            self.db.commit()
            return

        self.log(
            'processed {} with {} orbits for {} new lightpoints'.format(
                job.tic_id, len(job.file_observations), total_len
            )
        )

    def flush_observations(self):
        obs = [Observation(**kw) for kw in self.observations]
        self.db.session.add_all(obs)
        self.observations = []


    def flush(self):
        """Flush all caches to database"""
        # Insert all new lightcurves
        self.log('Flushing to database...')
        lcs = []
        observations = []
        for key, id_ in self.id_map.items():
            if id_ not in self.new_ids:
                continue
            tic_id, ap_id, lc_type_id = key
            lc = Lightcurve(
                id=id_,
                tic_id=tic_id,
                aperture_id=ap_id,
                lightcurve_type_id=lc_type_id
            )
            lcs.append(lc)

        self.db.session.add_all(lcs)
        self.db.commit()

        self.log('Sending {} lightpoints'.format(len(self.packer)), level='info')
        self.packer.serialize_to_database(self.db)
        self.flush_observations()
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

                if len(self.packer) > 10**6:
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
