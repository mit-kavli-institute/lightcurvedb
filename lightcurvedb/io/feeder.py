"""
This module describes multiprocessing queues in order to quickly feed
IO greedy processes. The functions described here can quickly spawn
multiple SQL sessions, use with caution.
"""
from multiprocessing import Manager, Pool, Process

import numpy as np
import sqlalchemy as sa
from sqlalchemy.exc import InternalError

from lightcurvedb import Lightcurve, db_from_config
from lightcurvedb import models as m
from lightcurvedb.exceptions import EmptyLightcurve, PrimaryIdentNotFound
from lightcurvedb.io.pipeline import db_scope
from lightcurvedb.io.procedures.procedure import get_lightcurve_data
from lightcurvedb.models.lightpoint import LIGHTPOINT_NP_DTYPES, Lightpoint


class LightcurveFeeder(Process):
    def __init__(
        self,
        columns,
        id_queue,
        result_queue,
        stmt_func,
        config=None,
        process_kwargs=None,
    ):
        super(LightcurveFeeder, self).__init__()
        self.daemon = True
        self.id_queue = id_queue
        self.result_queue = result_queue
        self.columns = columns
        self.stmt_func = stmt_func
        self.config = config

    def run(self):
        id_ = self.id_queue.get()
        with db_from_config(self.config) as db:
            while id_ is not None:
                # Grab lightcurve data
                stmt = self.stmt_func(id_, *self.columns)
                try:
                    lc = db.lightcurves.get(id_)
                    if lc is None:
                        raise PrimaryIdentNotFound
                    result = {
                        "id": lc.id,
                        "tic_id": lc.tic_id,
                        "aperture": lc.aperture_id,
                        "lightcurve_type": lc.lightcurve_type_id,
                    }
                    data = np.array(
                        list(map(tuple, db.execute(stmt))),
                        dtype=[
                            (column, LIGHTPOINT_NP_DTYPES[column])
                            for column in self.columns
                        ],
                    )
                    if len(data) == 0:
                        raise EmptyLightcurve
                    result["data"] = data
                except EmptyLightcurve:
                    result["error"] = "No lightpoints found, empty lightcurve"
                except InternalError:
                    # No data found!
                    db.rollback()
                    result["error"] = "No lightpoints found, empty lightcurve"
                except PrimaryIdentNotFound:
                    result = {
                        "error": f"No lightcurve found for identifier {id_}",
                        "id": id_,
                    }
                except Exception as e:
                    # Catch all, clean queues and exit
                    result = {"error": f"Encountered terminating error: {e}"}
                    break
                finally:
                    self.result_queue.put(result)
                    self.id_queue.task_done()
                    id_ = self.id_queue.get()

        self.id_queue.task_done()


def _yield_data(func, columns, ids, db_config_override=None, n_threads=None):
    n_processes = n_threads if n_threads else 1
    workers = []
    m = Manager()
    id_queue = m.Queue()
    result_queue = m.Queue(maxsize=1000)
    columns = columns if columns else Lightpoint.get_columns()

    # Filter out input signals which are used for IO control
    good_ids = list(filter(lambda i: i is not None, ids))

    for id_ in good_ids:
        id_queue.put(id_)

    for _ in range(n_processes):
        worker = LightcurveFeeder(columns, id_queue, result_queue, func)
        worker.start()
        workers.append(worker)
        id_queue.put(None)  # Kill sig

    for _ in range(len(good_ids)):
        yield result_queue.get()
        result_queue.task_done()

    # Clean up
    for worker in workers:
        worker.join()


def yield_lightcurve_data(
    ids, db_config_override=None, n_threads=None, columns=None
):
    """
    Iterate over lightcurve data given a list of lightcurve ids.
    The underlying query is performed asynchronously with a configurable
    number of readers.

    Parameters
    ----------
    ids: iterable sequence of ints
        The lightcurve ids to query for.
    db_config_override: Pathlike, optional
        Configuration override for the lightcurve readers.
    n_threads: int, optional
        The number of threads working on querying for lightcurves.
        By default this parameter is 1.
    columns: list of strings
        A list of lightpoint columns to return. By default all Lightpoint
        columns are queried.

    Returns
    -------
    np.ndarray
        A structured numpy 2D array. The array can be accessed by keywords
        either provided in the columns argument or any Lightpoint column
        name.
    """
    for data in _yield_data(
        get_lightcurve_data,
        columns,
        ids,
        db_config_override=db_config_override,
        n_threads=n_threads,
    ):
        yield data


def yield_best_aperture_data(
    tic_ids, db_config_override=None, n_threads=None, columns=None
):
    """
    Iterate over lightcurve data given a list of TIC ids. The TIC ids are
    translated through BestApertureMap and through KSPMagnitude types
    The underlying query is performed asynchronously with a configurable
    number of readers.

    Parameters
    ----------
    tic_ids: iterable sequence of ints
        The TIC identifiers to query for.
    db_config_override: Pathlike, optional
        Configuration override for the lightcurve readers.
    n_threads: int, optional
        The number of threads working on querying for lightcurves.
        By default this parameter is 1.
    columns: list of strings
        A list of lightpoint columns to return. By default all Lightpoint
        columns are queried.

    Returns
    -------
    np.ndarray
        A structured numpy 2D array. The array can be accessed by keywords
        either provided in the columns argument or any Lightpoint column
        name.
    """
    # Convert tic_ids to list of lightcurve ids
    ids = []
    with db_from_config(db_config_override) as db:
        q = db.lightcurves_from_best_aperture(resolve=False).filter(
            Lightcurve.tic_id.in_(tic_ids),
            Lightcurve.lightcurve_type_id == "KSPMagnitude",
        )
        for lc in q.all():
            ids.append(lc.id)

    for data in _yield_data(
        get_lightcurve_data,
        columns,
        ids,
        db_config_override=db_config_override,
        n_threads=n_threads,
    ):
        yield data


@db_scope()
def fetch_best_orbit_baseline(db, job):
    tic_id, columns = job
    best_orbit = m.BestOrbitLightcurve
    lc = m.OrbitLightcurve
    dtype = [(col, LIGHTPOINT_NP_DTYPES[col]) for col in columns]

    q = (
        sa.select(lc.id)
        .join(
            best_orbit,
            sa.and_(
                best_orbit.orbit_id == lc.orbit_id,
                best_orbit.aperture_id == lc.aperture_id,
                best_orbit.lightcurve_type_id == lc.lightcurve_type_id,
                best_orbit.tic_id == lc.tic_id,
            ),
        )
        .where(lc.tic_id == tic_id)
    )
    ids = [id for id, in db.execute(q)]

    lp_q = (
        sa.select(*[getattr(m.Lightpoint, col) for col in columns])
        .where(m.Lightpoint.lightcurve_id.in_(ids))
        .order_by(m.Lightpoint.cadence)
    )

    return tic_id, np.array(db.execute(lp_q).fetchall(), dtype=dtype)


def yield_best_lightcurve_data(
    tic_ids, db_override=None, n_threads=None, columns=None
):
    if columns is None or len(columns) == 0:
        columns = [
            "cadence",
            "barycentric_julian_date",
            "data",
            "error",
            "quality_flag",
        ]
    jobs = ((tic_id, columns) for tic_id in tic_ids)
    with Pool(n_threads) as pool:
        results = pool.imap_unordered(fetch_best_orbit_baseline, jobs)
        yield from results
