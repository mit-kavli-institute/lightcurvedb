"""
This module describes the best-orbit lightcurve manager subclasses
"""
from functools import partial
from multiprocessing import Pool

import numpy as np
from sqlalchemy import and_, func, select
from sqlalchemy.dialects.postgresql import aggregate_order_by

from lightcurvedb import db_from_config
from lightcurvedb.core.tic8 import TIC8_DB, one_off
from lightcurvedb.managers.manager import BaseManager
from lightcurvedb.models import (
    BestOrbitLightcurve,
    Lightpoint,
    OrbitLightcurve,
)
from lightcurvedb.models.lightpoint import LIGHTPOINT_NP_DTYPES


def _np_dtype(*cols):
    for col in cols:
        yield col, LIGHTPOINT_NP_DTYPES[col]


def _agg_lightpoint_col(*cols):
    aggs = tuple(
        func.array_agg(aggregate_order_by(col, Lightpoint.cadence.asc()))
        for col in cols
    )
    return aggs


def _load_best_lightcurve(db_config, lp_q, id_col, tic_id):
    with db_from_config(db_config) as db:
        q = (
            db.query(OrbitLightcurve.id)
            .join(
                BestOrbitLightcurve,
                and_(
                    BestOrbitLightcurve.orbit_id == OrbitLightcurve.orbit_id,
                    BestOrbitLightcurve.aperture_id
                    == OrbitLightcurve.aperture_id,
                    BestOrbitLightcurve.lightcurve_type_id
                    == OrbitLightcurve.lightcurve_type_id,
                    BestOrbitLightcurve.tic_id == OrbitLightcurve.tic_id,
                ),
            )
            .filter(OrbitLightcurve.tic_id == tic_id)
        )
        ids = [id_ for id_, in q]
        q = lp_q.filter(id_col.in_(ids))
        results = db.execute(q)
        return tic_id, results.fetchall()


def _baked_best_lightcurve(db_config, tic_id):
    with db_from_config(db_config) as db:
        q = (
            db.query(
                OrbitLightcurve.id,
            )
            .join(
                BestOrbitLightcurve,
                BestOrbitLightcurve.orbitlightcurve_join_condition(),
            )
            .filter(OrbitLightcurve.tic_id == tic_id)
        )
        ids = [id_ for id_, in q]
        q = (
            db.query(
                OrbitLightcurve.tic_id,
                Lightpoint.cadence_array(),
                Lightpoint.barycentric_julian_date_array(),
                Lightpoint.data_array(),
                Lightpoint.error_array(),
                Lightpoint.x_centroid_array(),
                Lightpoint.y_centroid_array(),
                Lightpoint.quality_flag_array(),
            )
            .join(OrbitLightcurve.id == Lightpoint.lightcurve_id)
            .filter(Lightpoint.lightcurve_id.in_(ids))
        )
        return q.all()[0]


class BestLightcurveManager(BaseManager):
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

    def __init__(self, db_config, normalize=True):
        """
        Initialize a best-lightcurve manager.

        Parameters
        ----------
        db_config: pathlike
            A path to an lcdb configuration file
        normalize: bool, optional
            If true, normalize returned lightcurves to their
            corresponding tmag values.
        """
        template = select(
            Lightpoint.lightcurve_id,
            *_agg_lightpoint_col(
                Lightpoint.cadence,
                Lightpoint.barycentric_julian_date,
                Lightpoint.data,
                Lightpoint.error,
                Lightpoint.x_centroid,
                Lightpoint.y_centroid,
                Lightpoint.quality_flag,
            )
        ).group_by(Lightpoint.lightcurve_id)
        self.normalize = normalize
        self.query_func = partial(
            _load_best_lightcurve,
            db_config,
            template,
            Lightpoint.lightcurve_id,
        )
        super().__init__(db_config, template, Lightpoint.lightcurve_id)

    def normalize_lightpoints(self, tmag, lp):
        """
        Normalize the lightpoint array to a common tmag. If thie manager
        instance was set to not normalize then this function will be a simple
        pass through.

        If set to normalize, the original lp array will be mutated.

        Parameters
        ----------
        tmag: float
            The common tmag
        lp: np.ndarray
            A structured lightpoint array

        Returns
        -------
        np.ndarray
            The modified lightpoint array.
        """
        if not self.normalize:
            return lp

        mask = lp["quality_flag"] == 0
        median = np.nanmedian(lp[mask]["data"])
        offset = median - tmag
        lp["data"] -= offset

        return lp

    def load(self, tic_id):
        """
        Attempt to load the given tic_id.
        """
        if self.normalize:
            tmag = one_off(tic_id, "tmag")
        else:
            tmag = None

        _, result = self.query_func(tic_id)
        lps = []
        for id_, *data in result:
            lp = self.interpret_data(data)
            lps.append(self.normalize_lightpoints(tmag, lp))

        self._cache[tic_id] = np.concatenate(lps)

    def interpret_data(self, result):
        """
        Cast the aggregate lightpoint arrays into a single
        structured numpy array.

        Parameters
        ----------
        result: List[List]
            The returned row containing aggregates.

        Returns
        -------
        np.ndarray
        """
        arr = np.array(
            list(zip(*result)),
            dtype=list(
                _np_dtype(
                    "cadence",
                    "barycentric_julian_date",
                    "data",
                    "error",
                    "x_centroid",
                    "y_centroid",
                    "quality_flag",
                )
            ),
        )
        return arr

    def parallel_load(self, tic_ids, n_readers=None):
        if n_readers is None:
            n_readers = min([16, len(tic_ids)])

        if self.normalize:
            with TIC8_DB() as tic8:
                t = tic8.ticentries.c
                result = tic8.execute(
                    select(t.id, t.tmag).filter(t.id.in_(tic_ids))
                )
                tmag_map = dict(result.fetchall())

        else:
            tmag_map = {}

        with Pool(n_readers) as pool:
            f = partial(_baked_best_lightcurve, self.db_config)
            all_results = pool.imap_unordered(f, tic_ids)
            for tic_id, result in all_results:
                lps = []
                for id_, *data in result:
                    lp = self.interpret_data(data)
                    tmag = tmag_map.get(tic_id, None)
                    lps.append(self.normalize_lightpoints(tmag, lp))

                self._cache[tic_id] = np.concatenate(lps)
