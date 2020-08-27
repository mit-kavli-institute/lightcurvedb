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
    @collection.replaces(1)
    def append(self, lightpoint):
        cadence = self.__extr_cadence__(lightpoint)
        if cadence in self.data:
            cur_lightpoint = self.data[cadence]
            cur_lightpoint.update_with(lightpoint)
        else:
            self.data[cadence] = lightpoint 

    def add(self, data):
        lightpoint = self.__clean__(data)
        self.append(lightpoint)

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

    # Begin setters
    # Don't support setting of cadences this way
    def __scalar_or_array__assign__(self, col, values):
        """
        Assign the values to the specified column. If the values are a
        listlike attempt to assign via one-to-one assignment. If a scalar,
        set all values to the given value.

        Parameters
        ----------
        col : str
            The column of values to assign.
        values : scalar or list
            The value(s) to assign to the specified ``col``.

        Raises
        ------
        IndexError:
            Raised if ``len(to_assign) != target_column``.
        """
        if isinstance(values, list):
            if not len(values) == len(self):
                raise IndexError(
                    'Was given assigments of length {} but collection only has {}'.format(len(values), len(self))
                )
            for value, lp in zip(values, self):
                setattr(lp, col, value)
        else:
            # Given a scalar
            for lp in self:
                setattr(lp, col, value)

    @barycentric_julian_date.setter
    def barycentric_julian_date(self, values):
        self.__scalar_or_array__assign__(
            'barycentric_julian_date',
            values
        )

    @values.setter
    def values(self, _values):
        self.__scalar_or_array__assign__(
            'data',
            values
        )

    @errors.setter
    def errors(self, values):
        self.__scalar_or_array__assign__(
            'error',
            values
        )

    @x_centroids.setter
    def x_centroids(self, values):
        raise NotImplementedError

    @y_centroids.setter
    def y_centroids(self, values):
        raise NotImplementedError

    @quality_flags.setter
    def quality_flags(self, values):
        raise NotImplementedError
