from lightcurvedb.models import Lightcurve, Lightpoint
from sqlalchemy import Integer
from sqlalchemy.sql import select, func, cast


def remove_old_points_q(lightcurves):
    """
    Create a query that deletes existing lightcurve points
    """
    q = Lightpoint.__table__.delete().where(
        Lightpoint.lightcurve_id.in_(
            set(lc.id for lc in lightcurves)
        )
    )
    return q


def async_h5_merge(config, job_queue, data_queue, time_corrector):
    """
    Merge h5 with given lightcurves within the given TICs.
    """
    pass
