import time
from collections import defaultdict
from contextlib import contextmanager
from multiprocessing import Process
from queue import Empty

from loguru import logger


class BufferedDatabaseIngestor(Process):
    job_queue = None
    name = "Worker"
    buffer_order = []

    def __init__(self, db, name, job_queue):
        super().__init__(daemon=True, name=name)
        self.db_session = db
        self.name = name
        self.job_queue = job_queue
        self.log("Initialized")
        self.buffers = defaultdict(list)
        self.telemetry = defaultdict(int)

    def log(self, msg, level="debug"):
        full_msg = f"{self.name} {msg}"
        getattr(logger, level, logger.debug)(full_msg)

    def reset_telemetry(self):
        self.buffers = defaultdict(int)

    def log_telemetry(self):
        for name, elapsed in self.telemetry.items():
            self.log(f"{name} took {elapsed:.2f}s")

    @contextmanager
    def record_elapsed(self, telemetry_type, *resources):
        t0 = time.time()
        try:
            yield resources
        finally:
            elapsed = time.time() - t0
            self.telemetry[telemetry_type] += elapsed

    def _create_db(self):
        self.db = self.db_session

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
                    f"and performing backoff. {tries} tries remaining.",
                    level="warning",
                )
                db.rollback()
                wait_time = 2 ** (5 - tries)
                time.sleep(wait_time)
                tries -= 1
        if tries == 0:
            raise RuntimeError(f"{self.name} could not push payload. Exciting")

        db.commit()
        self.reset_telemetry()

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

        job = self.job_queue.get()
        self._execute_job(job)
        while not self.job_queue.empty():
            try:
                job = self.job_queue.get(timeout=120)
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
