"""
This module describes multiprocessing queues in order to quickly feed
IO greedy processes. The functions described here can quickly spawn
multiple SQL sessions, use with caution.
"""
from multiprocessing import Pool

from lightcurvedb.managers.bestlightcurves import BestLightcurveManager


def fetch_best_orbit_baseline(job):
    config, tic_id = job
    lm = BestLightcurveManager(config=config)

    return tic_id, lm[tic_id]


def yield_best_lightcurve_data(
    tic_ids, db_override=None, n_threads=None, columns=None
):
    if columns is None or len(columns) == 0:
        columns = [
            "cadences",
            "barycentric_julian_dates",
            "data",
            "errors",
            "quality_flags",
        ]
    with Pool(n_threads) as pool:
        jobs = [(db_override, tic_id) for tic_id in tic_ids]
        results = pool.imap_unordered(fetch_best_orbit_baseline, jobs)
        yield from results
