try:
    # Python agnostic import of Queue
    import queue
except ImportError:
    import Queue as queue

from collections import deque, namedtuple
from multiprocessing import Process
from sqlalchemy.sql.expression import bindparam
from datetime import datetime
from psycopg2.errors import DeadlockDetected
from sqlalchemy.exc import OperationalError, IntegrityError
from time import sleep
from random import random
import pandas as pd
import numpy as np
import os

from lightcurvedb.models import Lightcurve, Observation, Orbit
from lightcurvedb.util.logger import lcdb_logger as logger
from lightcurvedb import db_from_config


class TransactionTime(object):
    def __init__(self, n_rows, time_start, time_end):
        if not isinstance(n_rows, int):
            raise ValueError(
                'Received {} instead of an integer'.format(n_rows)
            )
        self.n_rows = n_rows
        self.time_start = time_start
        self.time_end = time_end

    @property
    def elapsed(self):
        return self.time_end - self.time_start

    @property
    def seconds_elapsed(self):
        return self.elapsed.total_seconds()

    @property
    def throughput(self):
        return self.n_rows / self.seconds_elapsed


class TransactionHistory(object):
    def __init__(self, deque_size):
        self.timing_buffer = deque(maxlen=deque_size)

    def new_timing(self, n_rows, t0, t1):
        tt = TransactionTime(n_rows, t0, t1)
        self.timing_buffer.append(tt)

    def get_throughput_by_n_rows(self):
        """Convert buffer of TransactionTime to two arrays. One
        representing the past history of ``n_rows`` the other
        representing the correlated ``throughput``.
        """
        n_row_history = []
        throughput_history = []
        for tt in list(self.timing_buffer):
            n_row_history.append(tt.n_rows)
            throughput_history.append(tt.throughput)

        return np.array(n_row_history), np.array(throughput_history)

    def current_trend(self):
        """
        Calculate the recent trend of n_rows vs throughput.

        Notes
        -----
        If there is less than 2 timing objects, return 2.0 as the slope
        as a `jumpstart` to finding a good throughput size.
        """

        if len(self.timing_buffer) < 2:
            return 2.0

        x, y = self.get_throughput_by_n_rows()

        slope, _ = np.polyfit(
            x, y,
            1  # Strictly linear
        )
        return slope

    def get_new_buf_size(self, current_buf_size):
        slope = self.current_trend()
        new_buf_size = current_buf_size * (1.0 + slope)

        # Clip values against nonsense or dangerous values
        if new_buf_size < 1:
            return 1.0
        if new_buf_size > 1000:
            return 1000
        return int(new_buf_size)


class DBLoader(Process):
    """
    This process attempts to load the database in chunks. Each instance
    is aware of its transaction history and timings and can adjust its
    internal buffer to increase throughput.

    Observations are buffered infinitely. Their upsertion times are measured
    in milliseconds even with 10,000s of rows.
    """

    def __init__(self, db_config, lightcurve_queue, **process_kwargs):
        super(DBLoader, self).__init__(**process_kwargs)
        self.db = db_from_config(
            db_config,
            executemany_mode='values',
            executemany_values_page_size=10000,
            executemany_batch_page_size=500
        )
        self.queue = lightcurve_queue
        self.insert_history = TransactionHistory(100)
        self.update_history = TransactionHistory(100)

        # Set initial weights
        self.insert_weight = 10.0
        self.update_weight = 10.0
        self.cur_n_insert_rows = 10
        self.cur_n_update_rows = 10

        # Buffers
        self.insert_buffer = list()
        self.update_buffer = list()
        self.observation_buffer = list()

        self.set_name()

    def log(self, msg, level='debug'):
        getattr(logger, level)('{}: {}'.format(self.name, msg))

    def set_name(self):
        self.name = 'Ingestion Worker {}'.format(os.getpid())

    def flush_insert(self):
        q = Lightcurve.__table__.insert().values({
            Lightcurve.tic_id: bindparam('tic_id'),
            Lightcurve.aperture_id: bindparam('aperture_id'),
            Lightcurve.lightcurve_type_id: bindparam('lightcurve_type_id'),
            Lightcurve.cadences: bindparam('cadences'),
            Lightcurve.bjd: bindparam('bjd'),
            Lightcurve.values: bindparam('values'),
            Lightcurve.errors: bindparam('errors'),
            Lightcurve.x_centroids: bindparam('x_centroids'),
            Lightcurve.y_centroids: bindparam('y_centroids'),
            Lightcurve.quality_flags: bindparam('quality_flags')
        })
        t0 = datetime.utcnow()
        self.db.session.execute(
            q,
            self.insert_buffer
        )
        t1 = datetime.utcnow()
        self.insert_history.new_timing(
            len(self.insert_buffer),
            t0,
            t1
        )
        new_insert_buffer = self.insert_history.get_new_buf_size(
            len(self.insert_buffer)
        )

        self.log(
            'inserted {} rows. Setting new buffer from {} to {}'.format(
                len(self.insert_buffer),
                self.cur_n_insert_rows,
                new_insert_buffer
            ),
            level='info'
        )

        self.insert_buffer = list()
        self.cur_n_insert_rows = new_insert_buffer

    def flush_update(self):
        q = Lightcurve.__table__.update().\
            where(Lightcurve.id == bindparam('_id')).\
            values({
                Lightcurve.cadences: bindparam('cadences'),
                Lightcurve.bjd: bindparam('bjd'),
                Lightcurve.values: bindparam('values'),
                Lightcurve.errors: bindparam('errors'),
                Lightcurve.x_centroids: bindparam('x_centroids'),
                Lightcurve.y_centroids: bindparam('y_centroids'),
                Lightcurve.quality_flags: bindparam('quality_flags')
            })
        t0 = datetime.utcnow()
        self.db.session.execute(
            q,
            self.update_buffer
        )
        t1 = datetime.utcnow()
        self.update_history.new_timing(
            len(self.update_buffer),
            t0,
            t1
        )
        new_update_buffer = self.update_history.get_new_buf_size(
            len(self.update_buffer)
        )

        self.log(
            'updated {} rows. Setting new buffer from {} to {}'.format(
                len(self.update_buffer),
                self.cur_n_update_rows,
                new_update_buffer
            ),
            level='info'
        )

        self.update_buffer = list()
        self.cur_n_update_rows = new_update_buffer

    def flush_observations(self):
        """Prevent duplication of observations and create upsert statement"""
        df = pd.DataFrame(self.observation_buffer)
        df = df.set_index(['tic_id', 'orbit_id'])
        df = df[~df.index.duplicated(keep='last')]
        df = df.reset_index()

        # Since it is likely other parallel ingestors will alter the same
        # observation row, catch and retry when encountering Deadlock errors.
        while True:
            try:
                self.db.session.execute(
                    Observation.upsert_dicts(),
                    df.to_dict('records')
                )
                self.observation_buffer = list()
                break
            except (DeadlockDetected, OperationalError):
                self.log('retrying observation upsert')
                self.db.rollback()
                wait = random()
                sleep(wait)

    def run(self):
        # Enter into database session and begin ingesting, setup
        # variables needed for this run-scope.
        self.set_name()
        first_ingestion = True
        self.db.open()
        orbits = self.db.orbits.all()
        orbit_id_map = {
            o.orbit_number: o.id for o in orbits
        }
        while True:
            # Pull from queue until timeout
            altered_db = False
            try:
                if not first_ingestion:
                    lightcurve_kw = self.queue.get(timeout=5)
                else:
                    lightcurve_kw = self.queue.get(block=True)
                observations = lightcurve_kw.pop('observations')
                for obs in observations:
                    obs['orbit_id'] = orbit_id_map[int(obs['orbit'])]
                    del obs['orbit']
                self.observation_buffer += observations

                new_lightcurve = (
                    '_id' not in lightcurve_kw or
                    lightcurve_kw['_id'] is None or
                    lightcurve_kw['_id'] < 0
                )

                if not new_lightcurve:
                    self.update_buffer.append(lightcurve_kw)
                else:
                    lightcurve_kw['id'] = lightcurve_kw['_id']
                    del lightcurve_kw['_id']
                    self.insert_buffer.append(
                        lightcurve_kw
                    )

                self.queue.task_done()

                # Check buffers and emit if needed
                if len(self.insert_buffer) >= self.cur_n_insert_rows:
                    self.flush_insert()
                    altered_db = True
                if len(self.update_buffer) >= self.cur_n_update_rows:
                    self.flush_update()
                    altered_db = True

                if altered_db:
                    self.db.commit()
                    self.flush_observations()
                    self.db.commit()
            except queue.Empty:
                self.log('debug', 'timed out. Assuming no more data')
                break
        # Clean up any straggling data
        self.flush_insert()
        self.flush_update()
        self.flush_observation()
        self.db.commit()
        self.db.close()
