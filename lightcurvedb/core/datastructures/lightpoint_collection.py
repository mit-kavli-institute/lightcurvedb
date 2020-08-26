"""
This module describes custom collection objects for handling
many-to-one/one-to-many/many-to-many relations in SQLAlchemy.
"""
from sqlalchemy.orm.collections import Collection
from lightcurvedb.models.lightpoint import Lightpoint


class MassTrackedLightpoints(Collection):
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

        self.data = []

    def append(self, lightpoint):
        if isinstance(lightpoint, dict):
            cadence = lightpoint['cadence']
        elif isinstance(lightpoint, Lightpoint):
            cadence = lightpoint.cadence
        else:
            raise ValueError(
                "Could not append type {}".format(type(lightpoint))
            )
        raise NotImplementedError
