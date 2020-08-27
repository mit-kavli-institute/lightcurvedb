"""
This module describes custom collection objects for handling
many-to-one/one-to-many/many-to-many relations in SQLAlchemy.
"""
from sqlalchemy.orm.collections import collection
from lightcurvedb.models.lightpoint import Lightpoint


class MassTrackedLightpoints(object):
    """
    Track the lightpoints and manage insertions, deletions, and
    modifications.

    Allows either Lightpoint instances or dict-like instances to be
    used in data modifications.
    """

    __emulates__ = list

    def __init__(self):

        self._to_add = set()
        self._to_update = set()
        self._to_remove = set()

        self.data = dict()

    def __len__(self):
        return len(self.data)

    def __getitem__(self, key):
        return self.data[key]

    @collection.iterator
    def __iter__(self):
        for c in sorted(self.data.keys()):
            yield self.data[c]

    def __extr_cadence__(self, lightpoint):
        if isinstance(lightpoint, dict):
            cadence = lightpoint['cadence']
        elif isinstance(lightpoint, Lightpoint):
            cadence = lightpoint.cadence
        else:
            raise ValueError(
                "Could not get cadence of type {}".format(type(lightpoint))
            )
        return cadence

    def __clean__(self, lightpoint):
        if isinstance(lightpoint, Lightpoint):
            return lightpoint
        return Lightpoint(**lightpoint)

    def __contains__(self, lightpoint):
        return self.__extr_cadence__(lightpoint) in self.data

    @collection.appender
    def append(self, lightpoint):
        cadence = self.__extr_cadence__(lightpoint)
        if cadence in self.data:
            cur_lightpoint = self.data[cadence]
            cur_lightpoint.update_with(lightpoint)
        else:
            self.data[cadence] = self.__clean__(lightpoint)

    @collection.remover
    def remove(self, lightpoint):
        cadence = self.__extr_cadence__(lightpoint)
        del self.data[cadence]

    def extend(self, lightpoints):
        for lp in lightpoints:
            self.append(lp)

    @property
    def cadences(self):
        return [lp.cadence for lp in self]

    @property
    def barycentric_julian_date(self):
        return [lp.barycentric_julian_date for lp in self]

    @property
    def bjd(self):
        return [lp.barycentric_julian_date for lp in self]

    @property
    def values(self):
        return [lp.data for lp in self]

    @property
    def errors(self):
        return [lp.error for lp in self]

    @property
    def x_centroids(self):
        return [lp.x for lp in self]

    @property
    def y_centroid(self):
        return [lp.y for lp in self]

    @property
    def quality_flags(self):
        return [lp.quality_flag for lp in self]

