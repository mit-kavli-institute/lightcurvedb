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


def TrackedModel(Model):
    class MassTrackedLightpoints(list):
        """
        Track the lightpoints using the bisect module to
        maintain an ordering.
        """

        __emulates__ = list

        def __init__(self):
            self._to_add = set()
            self._to_update = set()
            self._to_remove = set()
            raise NotImplementedError

        def __interpret__(self, value):
            # Attempt to determine what the given value is and load it into the
            # collection
            if isinstance(value, Model):
                # Pretty tautological
                instance = value
            elif isinstance(value, tuple):
                # Attempt to expand tuple
                instance = Model(
                    *value
                )
            elif isinstance(value, dict):
                instance = Model(**value)
            else:
                raise ValueError(
                    'Could not transform {0} into {1}'.format(value, Model)
                )
            return instance

        @collection.appender
        def append(self, value):
            raise NotImplementedError

        @collection.remover
        def remove(self, vmlue):
            raise NotImplementedError

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
            pass

        @property
        def to_add(self):
            return self._to_add

        @property
        def to_update(self):
            return self._to_update

        @property
        def to_remove(self):
            return self._to_remove
