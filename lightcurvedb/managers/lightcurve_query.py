from collections import defaultdict
from functools import partial
import os
import multiprocessing as mp
from multiprocessing.managers import SyncManager
from .sql_worker import SQLWorker, Job
from .locking import QLPBarrier
from sqlalchemy.ext.serializer import dumps
import logging


logger = logging.getLogger('Manager')


def set_dict():
    """Helper to create default dictionaries with set objects"""
    return defaultdict(set)


class LightcurveManager(object):
    """LightcurveManager. A class to help manager and keep track of
    lists of lightcurve objects.
    """

    def __init__(self, lightcurves):
        """__init__.

        Parameters
        ----------
        lightcurves :
            An iterable collection of lightcurves to manage.
        """
        self.tics = set_dict()
        self.apertures = set_dict()
        self.types = set_dict()
        self.id_map = dict()

        for lightcurve in lightcurves:
            self.tics[lightcurve.tic_id].add(lightcurve.id)
            self.apertures[lightcurve.aperture.name].add(lightcurve.id)
            self.types[lightcurve.lightcurve_type.name].add(lightcurve.id)
            self.id_map[lightcurve.id] = lightcurve

        self.searchables = (
            self.tics,
            self.apertures,
            self.types
        )

    def __repr__(self):
        return '<LightcurveManager: {} lightcurves>'.format(len(self))

    def __getitem__(self, key):
        """__getitem__.

        Parameters
        ----------
        key :
            The key to search for
        Raises
        ------
        KeyError
            If the key is not found within the LightcurveManager
        """
        for searchable in self.searchables:
            if key in searchable:
                ids = searchable[key]
                if len(ids) == 1:
                    # Singular, just return the lightcurve
                    id = next(iter(ids))
                    return self.id_map[id]
                return LightcurveManager([self.id_map[id_] for id_ in ids])

        raise KeyError(
            'The keyword \'{}\' was not found in the query'.format(key)
        )

    def __len__(self):
        """__len__.
        The length of the manager in terms of number of stored lightcurves.
        """
        return len(self.id_map)

    def __iter__(self):
        """__iter__.
        Iterate over the stored lightcurves.
        """
        return iter(self.id_map.values())


class LightcurveDaemon(object):
    """LightcurveDaemon.
    """
    def __init__(self, session, max_queue=0, n_psql_workers=1):
        """__init__.

        Parameters
        ----------
        session :
            DB object to give to workers
        max_queue :
            Maximum Query Queue size before blocks occur
        n_psql_workers :
            Number of PSQL workers. Anything < 1 will throw
            a ValueError

        Raises
        ------
        ValueError :
            A ValueError is raised if n_psql_workers is < 1
        """
        if n_psql_workers < 1:
            raise ValueError(
                'Number of PSQL workers cannot be < 1. Given {}'.format(
                    n_psql_workers
                )
            )
        self._session = session
        self._sync = SyncManager()
        self._query_queue = mp.JoinableQueue(max_queue)
        self._n_psql_workers = n_psql_workers
        self._resultant_queue = None
        self._processes = []

        # Internal bookkeeping
        self._process_map = defaultdict(int)
        self._process_job_map = {}

    def open(self):
        """open.

        Prepares the LightcurveDaemon to receive jobs. Spawns the specified
        number of processes to consume SQL jobs.
        """
        pid = os.getpid()
        self._sync.start()
        self._resultant_queue = self._sync.dict()
        self._processes = [
            SQLWorker(
                self._session._url,
                self._query_queue,
                self._resultant_queue,
                name='Worker[{}]-{}'.format(pid, p),
                daemon=True
            )
            for p in range(self._n_psql_workers)
        ]

        for p in self._processes:
            p.start()

        logger.info('Manager started {} workers'.format(len(self._processes)))

    def close(self):
        """close.
        Signals the job queues that no more input will be given.
        The job queue will join and wait for any remaining jobs.
        Finally all processes will be joined and disposed of.
        """
        for p in self._processes:
            self.job_queue.join()
            p.join()
        self._processes = []

    def _make_job(self, job_id, source_process, q):
        serialized_q = dumps(q)
        job = Job(
            source_process,
            job_id,
            serialized_q,
            self._sync.Event()
        )
        logger.info('Manager received {} from {}'.format(job, source_process))
        return job

    def push(self, q):
        source_process = os.getpid()
        nth_job = self._process_map[source_process]
        job_id = 'job-{}-process-{}'.format(
            nth_job,
            source_process
        )
        job = self._make_job(
            job_id,
            source_process,
            q
        )
        self._process_map[job_id] = job
        self.job_queue.put(job)

        self._process_map[source_process] += 1
        return job.job_id


    def get(self, job_reference):
        result = self._process_map[job_reference]
        result.is_done.wait()
        return self._process_map.pop(job_reference)

    @property
    def job_queue(self):
        """job_queue.
        """
        return self._query_queue

    @property
    def result_queue(self):
        """result_queue.
        """
        return self._resultant_queue

    def __enter__(self):
        """__enter__.
        Allows for contextual "opening" of a daemon for easy
        bookkeeping.
        """
        self.open()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """__exit__.
        Allows for contextual "exiting" of a daemon for easy
        bookkeeping.

        Parameters
        ----------
        exc_type :
            exc_type
        exc_value :
            exc_value
        traceback :
            traceback
        """
        self.close()
        return self
