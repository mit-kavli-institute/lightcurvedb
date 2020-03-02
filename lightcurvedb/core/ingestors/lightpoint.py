from .lightcurve_ingestors import h5_to_matrices
from lightcurvedb.models import Lightcurve, Lightpoint
from sqlalchemy import Integer
from sqlalchemy.sql import select, func, cast
from sqlalchemy.dialects.postgresql import ARRAY
import bisect


def get_raw_h5(filepath):
    return list(h5_to_matrices(filepath))


def get_cadence_info(db, tics):
    lc_map_q = select(
        [
            Lightcurve.id,
            Lightcurve.min_cadence,
            Lightcurve.max_cadence
        ]
    ).where(Lightcurve.tic_id.in_(tics))

    return db.session.execute(lc_map_q)


def find_new_lightpoints(datablock, existing_lightcurve):

    # Assumes lightpoints are in ascending order with respect to
    # cadence and bjd
    lightpoints = existing_lightcurve.lightpoints

