from collections import defaultdict
from multiprocessing import Process
from queue import Empty

from loguru import logger

from lightcurvedb import db_from_config


class BufferedDatabaseIngestor(Process):
    job_queue = None
    name = "Worker"
    db_config = None
    db = None
    buffers = defaultdict(list)
    buffer_order = []

    def __init__(self, config, name, job_queue):
        super().__init__(daemon=True, name=name)
        self.db_config = config
        self.name = name
        self.job_queue = job_queue
        self.log("Initialized")

    def log(self, msg, level="debug"):
        full_msg = f"{self.name} {msg}"
        getattr(logger, level, logger.debug)(full_msg)

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

        for buffer_key in self.buffer_order:
            method_name = f"flush_{buffer_key}"
            flush_method = getattr(self, method_name)
            metric = flush_method(db)
            if metric is not None:
                metrics.append(metric)

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
        self.db = db_from_config(self.db_config)
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

        if self.any_data_buffered:
            self.log("Leftover data found in buffers, submitting")
            with self.db as db:
                self.flush(db)

        self.log("Finished, exiting main runtime")

    @property
    def should_flush(self):
        raise NotImplementedError

    @property
    def any_data_buffered(self):
        return any(len(self.buffers[key]) > 0 for key in self.buffer_order)
