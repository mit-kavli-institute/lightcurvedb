from sqlalchemy.orm.query import Query

from lightcurvedb.exceptions import LightcurveDBException
from lightcurvedb.models import Lightcurve
from lightcurvedb.managers.manager import manager_factory


BaseLightcurveManager = manager_factory(
    Lightcurve,
    'tic_id',
    'aperture_id',
    'lightcurve_type_id'
)


class IncongruentLightcurve(LightcurveDBException):
    """Raised when attempting to modify a lightcurve in a way such that
    its internal arrays become misaligned"""
    pass


class LightcurveManager(BaseLightcurveManager):
    """LightcurveManager. A class to help manager and keep track of
    lists of lightcurve objects.
    """

    array_attrs = [
        'cadences',
        'bjd',
        'values',
        'errors',
        'x_centroids',
        'y_centroids',
        'quality_flags']

    DEFAULT_RESOLUTION = {
        'KSPMagnitude': 'RawMagnitude'
    }

    def __repr__(self):
        return '<LightcurveManager: {0} lightcurves>'.format(len(self))

    @classmethod
    def from_q(cls, q):

        lm = cls([])

        if isinstance(q, Query):
            for lc in q.all():
                lm.add_defined_lightcurve(lc)
        else:
            # Assume q is an iterable...
            for lc in q:
                lm.add_defined_lightcurve(lc)

        return lm
