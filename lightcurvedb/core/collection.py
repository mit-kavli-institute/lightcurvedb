"""
This module describes custom collection objects for handling
many-to-one/one-to-many/many-to-many relations in SQLAlchemy.
"""
from sqlalchemy.orm.collections import collection
from collections import namedtuple


RawLightpoint = namedtuple(
    'RawLightpoint',
    [
        'cadence',
        'barycentric_julian_date',
        'data',
        'error',
        'x_centroid',
        'y_centroid',
        'quality_flag'
    ]
)


class CadenceTracked(list):
    """
    """

    __emulates__ = list

    def __init__(self):
        self._to_add = set()
        self._to_update = set()
        self._to_remove = set()
        self._internal_data = {}

    def __getattr__(self, attribute):
        """
        Called and no defined attribute has been defined. Assume that
        the user wants to grab the related attribute from the tracked
        items instead. Automatically order by cadence information.
        """
        values = [
            getattr(self[cadence], attribute) for cadence in self.cadences
        ]
        return values

    def __iter__(self):
        for cadence in self.cadences:
            yield self[cadence]

    @collection.appender
    def append(self, value):
        self._internal_data[value.cadence] = value

    @collection.remover
    def remove(self, value):
        del self._internal_data[value.cadence]

    def bulk_replace(
            self,
            values,
            existing_adaptor,
            new_adaptor,
            initiator=None
            ):
        """
        Bulk replaces the collection. For the database this means
        deleting all current related models and performing an insert.
        """
        raise NotImplementedError

    @property
    def cadences(self):
        """
        Always return cadences in ascending order
        """
        return sorted(self._internal_data.keys())
