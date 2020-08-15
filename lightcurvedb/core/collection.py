"""
This module describes custom collection objects for handling
many-to-one/one-to-many/many-to-many relations in SQLAlchemy.
"""

from sqlalchemy.orm.collections import Collection


class MassTrackedLightpoints(Collection):
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

    def append(self, value):
        raise NotImplementedError

    def remove(self, value):
        raise NotImplementedError

    def bulk_replace(self, values, existing_adaptor, new_adaptor, initiator=None):
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
