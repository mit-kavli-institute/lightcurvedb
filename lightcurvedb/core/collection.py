"""
This module describes custom collection objects for handling
many-to-one/one-to-many/many-to-many relations in SQLAlchemy.
"""
from numpy import array as np_array
from sqlalchemy.orm.collections import collection
from collections import namedtuple, OrderedDict


RawLightpoint = namedtuple(
    "RawLightpoint",
    [
        "cadence",
        "barycentric_julian_date",
        "data",
        "error",
        "x_centroid",
        "y_centroid",
        "quality_flag",
    ],
)


class CadenceKeyed(object):
    """
    Tracks objects that have an integer attribute named "cadence".
    Objects are kept in sorted order. Iteration over this object
    will return the objects in ascending cadence order. And each
    cadence is considered unique within this object.
    """

    __emulates__ = list

    def __init__(self, *initial_data):
        self._internal_data = OrderedDict()

        for data in sorted(initial_data, key=lambda d: d.cadence):
            self._internal_data[data.cadence] = data

    def __len__(self):
        return len(self._internal_data)

    def __contains__(self, key):
        """
        Checks if the given key is in any of the tracked cadences.

        Returns
        -------
        bool
            True if the cadence is tracked within the collection.
        """
        return key in self._internal_data

    def __iter__(self):
        """
        Yields tracked items in ascending cadence order.

        Yields
        ------
        obj
        """
        for cadence in self.cadences:
            yield self[cadence]

    def __getattr__(self, attribute):
        """
        Called and no defined attribute has been defined. Assume that
        the user wants to grab the related attribute from the tracked
        items instead. Automatically order by cadence information.
        """
        values = [self[cadence][attribute] for cadence in self.cadences]
        return np_array(values)

    def __getitem__(self, key):
        """
        Returns the object(s) at the given cadence(s). If ``key`` is iterable
        then a subset of objects will be returned.

        Parameters
        ----------
        key : int or iterable of integers

        Returns
        -------
        obj or CadenceKeyed collection of obj
            The wanted objects.

        Note
        ----
        ``key`` may not be a unique or ordered iterable. If so the objects
        will be returned in the given key order.
        """
        try:
            # Check to see if key is iterable
            relevant = iter(key)
            return CadenceKeyed(
                *[self._internal_data[k] for k in relevant if k in self]
            )
        except TypeError:
            # Key is scalar
            return self._internal_data[key]

    @collection.internally_instrumented
    def __setitem__(self, key, value):
        if not len(self) == len(value):
            raise ValueError(
                "attempted to assign data with length of {0} vs "
                "on collection with length of {1}.".format(
                    len(value), len(self)
                )
            )
        for lp, v in zip(self, value):
            lp[key] = v

    @property
    def cadences(self):
        """
        Return cadences in ascending order
        """
        return np_array(sorted(self._internal_data.keys()))


class CadenceTracked(CadenceKeyed):
    """
    Extend the ``CadenceKeyed`` class to allow for SQLAlchemy to
    utilize it as a ORM relational collection object.
    """

    def __init__(self, *initial_data):
        super(CadenceTracked, self).__init__(*initial_data)
        self._to_add = set()
        self._to_update = set()
        self._to_remove = set()

    @collection.appender
    @collection.replaces(1)
    def append(self, value):
        if value.cadence in self:
            previous = self[value.cadence]
        else:
            previous = None

        self._internal_data[value.cadence] = value
        return previous

    @collection.appender
    @collection.replaces(1)
    def add(self, value):
        return self.append(value)

    @collection.remover
    def remove(self, value):
        del self._internal_data[value.cadence]

    def extend(self, values):
        for value in values:
            self._internal_data[value.cadence] = value

    def bulk_replace(
        self, values, existing_adaptor, new_adaptor, initiator=None
    ):
        """
        Bulk replaces the collection. For the database this means
        deleting all current related models and performing an insert.
        """
        raise NotImplementedError
