import time
from collections import defaultdict
from multiprocessing import Process
from queue import Empty

from loguru import logger

from lightcurvedb import db_from_config


class BufferedDatabaseIngestor(Process):
    job_queue = None
    name = "Worker"
    buffer_order = []

    def __init__(self, config, name, job_queue):
        super().__init__(daemon=True, name=name)
        self.db_config = config
        self.name = name
        self.job_queue = job_queue
        self.log("Initialized")
        self.buffers = defaultdict(list)

    def log(self, msg, level="debug"):
        full_msg = f"{self.name} {msg}"
        getattr(logger, level, logger.debug)(full_msg)

    def _create_db(self):
        self.db = db_from_config(self.db_config)

    def _load_contexts(self):
        return

    def _execute_job(self, job):
        self.process_job(job)
        if self.should_flush:
            with self.db as db:
                self.flush(db)

    def _preflush(self, db):
        pass

    def _postflush(self, db):
        pass

    def process_job(self, job):
        raise NotImplementedError

    def flush(self, db):
        self._preflush(db)
        metrics = []
        tries = 5
        while tries > 0:
            try:
                for buffer_key in self.buffer_order:
                    method_name = f"flush_{buffer_key}"
                    flush_method = getattr(self, method_name)
                    metric = flush_method(db)
                    if metric is not None:
                        metrics.append(metric)
                # Successful push
                break
            except RuntimeError:
                self.log(
                    "Encountered deadlock state, rolling back "
                    f"and performing backoff. {tries} tries remaining."
                )
                db.rollback()
                wait_time = 2 ** (5 - tries)
                time.sleep(wait_time)
                tries -= 1
        if tries == 0:
            raise RuntimeError(f"{self.name} could not push payload. Exciting")

        db.commit()

        # Emplace metrics
        for metric in metrics:
            db.add(metric)

        db.commit()

        # Clear buffers
        for buffer_key in self.buffer_order:
            self.buffers[buffer_key] = []

        self._postflush(db)

    def run(self):
        self.log("Entering main runtime")
        self._create_db()
        self._load_contexts()
        while not self.job_queue.empty():
            try:
                job = self.job_queue.get(timeout=10)
                self._execute_job(job)
            except Empty:
                self.log("Timed out", level="error")
                break
            except KeyboardInterrupt:
                self.log("Received keyboard interrupt")
                break
            except Exception:
                self.log(
                    "Unhandled exception, cowardly exiting...",
                    level="exception",
                )
                break

        if self.any_data_buffered:
            self.log("Leftover data found in buffers, submitting")
            with self.db as db:
                self.flush(db)

        if self.job_queue.empty():
            self.log("Successfully finished all jobs", level="success")

        self.log("Finished, exiting main runtime")

    @property
    def should_flush(self):
        raise NotImplementedError

    @property
    def any_data_buffered(self):
        return any(len(self.buffers[key]) > 0 for key in self.buffer_order)
