"""
This module describes the best-orbit lightcurve manager subclasses
"""
import cachetools
import sqlalchemy as sa

from lightcurvedb import db_from_config, models
from lightcurvedb.managers.lightcurves import LightcurveManager
from lightcurvedb.util.constants import __DEFAULT_PATH__


class BestLightcurveManager(LightcurveManager):
    """
    A class for managing "best" lightcurves. This class is accessed by
    tic ids.

    Example
    -------
    ```python

    mgr = BestLightcurveManager(db_config, normalize=True)
    lc = mgr[38696575]  # Emits query

    lc["cadence"] # np.array([1, 2, 3, 4])
    lc["data"] # np.array([0.0, 10.0, 4.0, nan])
    lc["quality_flag"] # np.array([0, 0, 0, 1])
    ```
    """

    def __init__(self, config=None, cache_size=4096):
        self._config = __DEFAULT_PATH__ if config is None else config
        self._lightcurve_id_cache = cachetools.LFUCache(cache_size)
        self._lightpoint_cache = cachetools.LRUCache(cache_size)
        self._stellar_parameter_cache = cachetools.LRUCache(cache_size)

    def __getitem__(self, tic_id):
        return self.get_lightcurve(tic_id)

    def _resolve_lightcurve_ids_for(self, tic_id):
        q = (
            sa.select(
                models.ArrayOrbitLightcurve.tic_id,
                sa.func.array_agg(models.ArrayOrbitLightcurve.id),
            )
            .join(
                models.BestOrbitLightcurve,
                models.BestOrbitLightcurve.lightcurve_join(
                    models.ArrayOrbitLightcurve
                ),
            )
            .where(models.ArrayOrbitLightcurve.tic_id == tic_id)
            .group_by(models.ArrayOrbitLightcurve.tic_id)
        )

        with db_from_config(self._config) as db:
            for tic_id, ids in db.execute(q):
                yield tic_id, ids

    def _resolve_key(self, tic_id):
        try:
            ids = self._lightcurve_id_cache[tic_id]
        except KeyError:
            _, ids = list(self._resolve_lightcurve_ids_for(tic_id))[0]
        return ids

    def get_lightcurve(self, tic_id):
        ids = self._resolve_key(tic_id)
        return self.construct_lightcurve(ids)
