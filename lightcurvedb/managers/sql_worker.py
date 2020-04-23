from lightcurvedb.core.engines import init_engine
from lightcurvedb.core.base_model import QLPModel
from collections import namedtuple
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.serializer import loads
from multiprocessing import Process, Event


Job = namedtuple('Job', ['source_process', 'job_id', 'job', 'is_done'])


def make_job(job_id, source_process, work_to_do):
    done_event = Event()
    return Job(
        source_process=source_process,
        job_id=job_id,
        job=work_to_do,
        is_done=done_event
    )


class SQLWorker(Process):
    def __init__(self, url, sql_queue, resultant_queue, **kwargs):
        super(Process, self).__init__(**kwargs)
        self.url = url
        self._sql_queue = sql_queue
        self._resultant_queue = resultant_queue

    def run(self):
        # Create connection
        engine = init_engine(self.url)
        factory = sessionmaker(bind=engine)
        Session = scoped_session(factory)

        session = Session()
        while not self._sql_queue.empty():
            try:
                job = self._sql_queue.get()

                # We need to unpickle query object
                q = loads(job.job, QLPModel.metadata, session)
                self._resultant_queue.put(
                    q.all()
                )
            except Exception:
                # Cowardly reset database state since we don't
                # know what went wrong in this scope
                session.rollback()
            finally:
                # Cleanup, notify daemon that the task has been
                # completed and set the appropriate events for
                # any waiting consumers
                job.is_done.set()
                self._sql_queue.task_done()

        # Done processing
        session.commit()
        session.close()
