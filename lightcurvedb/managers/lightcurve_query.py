from abc import ABCMeta, abstractmethod
from collections import defaultdict, namedtuple
from functools import partial
from itertools import groupby

import numpy as np
import pandas as pd
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.serializer import dumps
from sqlalchemy.inspection import inspect
from sqlalchemy.orm.query import Query
from sqlalchemy.schema import UniqueConstraint
from sqlalchemy.sql.expression import bindparam

from lightcurvedb.exceptions import LightcurveDBException
from lightcurvedb.models import Lightcurve
from lightcurvedb.models.aperture import BestApertureMap


class AmbiguousIdentifierDeduction(LightcurveDBException):
    """Raised when resolving a lightcurve ID and an insufficent
    set of identifiers were passed to successfully determine a scalar
    ID or lack thereof.
    """
    pass


class DuplicateEntryException(LightcurveDBException):
    """Raised when attempting to add a lightcurve which already
    exists in a LightcurveManager context.
    """
    pass


class IncongruentLightcurve(LightcurveDBException):
    """Raised when attempting to modify a lightcurve in a way such that
    its internal arrays become misaligned"""
    pass


def set_dict():
    """Helper to create default dictionaries with set objects"""
    return defaultdict(set)


class Manager(object):
    """Base Manager object. Defines abstract methods to retrive, store,
    and update lightcurves."""
    __metaclass__ = ABCMeta
    __managed_class__ = None
    __uniq_tuple__ = None

    def __init__(self, initial_models):
        self._interior_data = dict()

    def __get_key__(self, model_inst):
        key = tuple(
            getattr(model_inst, col) for col in self.__uniq_tuple__
        )
        return key

    def get_model(self, val, *uniq_vals):
        key = tuple([val].extend(uniq_vals))
        return self._interior_data[key]

    def add_model(self, model_inst):
        """
        Add the model to be tracked by the Manager
        """
        _uniq_key = self.__get_key__(model_inst)
        if _uniq_key in self._interior_data:
            raise DuplicateEntryException()
        self._interior_data[_uniq_key] = model_inst

    def add_model_kw(self, **kwargs):
        pass


def manager_factory(sqlalchemy_model, uniq_col, *additional_uniq_cols):
    class Managed(Manager):
        __managed_class__ = sqlalchemy_model
        __uniq_tuple__ = tuple([uniq_col].extend(additional_uniq_cols))

    return Managed



class LightcurveManager(object):
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

    def __init__(self, lightcurves):
        """__init__.

        Parameters
        ----------
        lightcurves : iterable of ``Lightcurve`` instances
            An iterable collection of lightcurves to manage.
        """
        self.tics = set_dict()
        self.apertures = set_dict()
        self.types = set_dict()
        self.id_map = dict()

        self.cur_tmp_id = -1  # All IDs < 0 are 'new' lightcurves

        self.aperture_defs = {}
        self.type_defs = {}

        self._to_add = list()
        self._to_update = list()
        self._to_upsert = list()

        for lightcurve in lightcurves:
            self.tics[lightcurve.tic_id].add(lightcurve.id)
            self.apertures[lightcurve.aperture_id].add(lightcurve.id)
            self.types[lightcurve.lightcurve_type_id].add(lightcurve.id)
            self.id_map[lightcurve.id] = lightcurve

        self.searchables = (
            self.tics,
            self.apertures,
            self.types
        )

    def __repr__(self):
        return '<LightcurveManager: {} lightcurves>'.format(len(self))

    def __getitem__(self, key):
        """__getitem__.

        Arguments
        ----------
        key : obj
            The key to search for

        Raises
        ------
        KeyError
            If the key is not found within the LightcurveManager.
        """

        found_ids = []

        for searchable in self.searchables:
            if key in searchable:
                ids = searchable[key]
                found_ids.append(ids)
        # If found_ids is empty, raise an error
        if len(found_ids) == 0:
            raise KeyError(
                'The keyword \'{}\' was not found in the query'.format(key)
            )

        # Grab the union of all found ids. If it's a single id, return the
        # lightcurve
        result = set.intersection(*found_ids)
        if len(result) == 0:
            raise KeyError(
                'The keyword \'{}\' was not found in the query'.format(key)
            )
        if all(len(s) <= 1 for s in found_ids):
            id = next(iter(result))
            return self.id_map[id]
        elif len(result) > 1:
            return LightcurveManager([self.id_map[id_] for id_ in result])
        raise KeyError(
            'The keyword \'{}\' was not found in the query'.format(key)
        )

    def __len__(self):
        """
        The length of the manager in terms of number of stored lightcurves.

        Returns
        -------
        int
            The number of lightcurves managed.
        """
        return len(self.id_map)

    def __iter__(self):
        """
        Iterate over the stored lightcurves.

        Returns
        -------
        iterator
            An iterator over all the lightcurves within this manager.
        """
        return iter(self.id_map.values())

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

    def update_w_q(self, q):
        for lightcurve in q.all():
            self.add_defined_lightcurve(lightcurve)

    def resolve_id(self, tic_id, aperture, lightcurve_type):
        lc_by_tics = self.tics.get(tic_id, set())
        lc_by_aps = self.apertures.get(aperture, set())
        lc_by_types = self.types.get(lightcurve_type, set())

        try:
            return set.intersection(lc_by_tics, lc_by_aps, lc_by_types).pop()
        except KeyError:
            # Nothing to resolve
            return None

    def clear_tracked(self):
        self._to_add = list()
        self._to_update = list()
        self._to_upsert = list()

    def add_defined_lightcurve(self, lightcurve):
        """
        Tracks a lightcurve that has a defined ID. If such a lightcurve were
        to contain identifiers that already exist within the manager then
        the appropriate id will be assigned to the manager.

        Arguments
        ---------
        lightcurve : ``Lightcurve``
            The lightcurve to add to the manager.

        Raises
        ------
        ValueError
            The given lightcurve does not have a valid ID.

        Returns
        -------
        ``Lightcurve``
            The merged lightcurve as viewed by the manager.
        """

        id_check = self.resolve_id(
            lightcurve.tic_id,
            lightcurve.aperture_id,
            lightcurve.lightcurve_type_id
        )
        if id_check is None:
            if lightcurve.id:
                # Add the lightcurve
                id_ = lightcurve.id
            else:
                id_ = self.cur_tmp_id
                self.cur_tmp_id -= 1

            self.id_map[id_] = lightcurve
            self.tics[lightcurve.tic_id].add(id_)
            self.apertures[lightcurve.aperture_id].add(id_)
            self.types[lightcurve.lightcurve_type_id].add(id_)

            return lightcurve

        if id_check < 0 and not lightcurve.id:
            # Both lightcurves are "temporary", merge them
            cur_lc = self.id_map[id_check]
            cur_lc.merge(lightcurve)
            return cur_lc
        elif id_check < 0 and lightcurve.id:
            # Re-assign current id
            cur_lc = self.id_map[id_check]
            merged_lc = lightcurve.merge(cur_lc)
            self.id_map[lightcurve.id] = merged_lc

            # Remove old references
            del self.id_map[id_check]
            self.tics[lightcurve.tic_id].remove(id_check)
            self.apertures[lightcurve.aperture_id].remove(id_check)
            self.types[lightcurve.lightcurve_type_id].remove(id_check)
            return lightcurve
        elif id_check > 0 and not lightcurve.id:
            cur_lc = self.id_map[id_check]
            cur_lc.merge(lightcurve)
            return cur_lc
        else:
            # Impossible, lightcurve id has a defined conflict
            conflicting = self.id_map[id_check]
            msg = 'Given lightcurve has defined id {} but manager has {}'.format(
                    lightcurve.id,
                    id_check
            )
            msg += '\n{} has ({}, {}, {})\n'.format(
                    lightcurve.id,
                    lightcurve.tic_id,
                    lightcurve.aperture_id,
                    lightcurve.lightcurve_type_id
            )
            msg += '{} has ({}, {}, {})'.format(
                    conflicting.id,
                    conflicting.tic_id,
                    conflicting.aperture_id,
                    conflicting.lightcurve_type_id
            )

            raise ValueError(msg)

    def add(self, tic_id, aperture, lightcurve_type, **data):
        """Adds a new lightcurve to the manager. This will create a new
        Lightcurve model instance and track it for batch insertions.

        Arguments
        ---------
        tic_id : int
            The TIC Number for the new Lightcurve

        aperture : str
            The ``Aperture.name`` to be linked.

        lightcurve_type : str
            The ``LightcurveType.name`` to be linked.

        Raises
        ------
        DuplicateEntryException
            Raised when attempting to add a
            lightcurve that already contains the same tic, aperture, and type
            in order to avoid a PSQL Unique Contraint violation that will
            invalidate mass queries. Caveat: will only catch unique constraint
            violations within this Manager instance's context.

        Returns
        -------
        ``Lightcurve``
            The constructed Lightcurve object.
        """
        try:
            assert self.resolve_id(tic_id, aperture, lightcurve_type) is None
        except AssertionError:
            raise DuplicateEntryException(
                '{} already exists in the manager'.format(
                    (tic_id, aperture, lightcurve_type)
                )
             )
        checked_data = self.__validate__(tic_id, aperture, lightcurve_type, **data)

        # Update definitions
        self.tics[tic_id].add(self.cur_tmp_id)
        self.apertures[aperture].add(self.cur_tmp_id)
        self.types[lightcurve_type].add(self.cur_tmp_id)

        lc = Lightcurve(
            tic_id=tic_id,
            aperture_id=aperture,
            lightcurve_type_id=lightcurve_type,
            **checked_data
        )

        self.id_map[self.cur_tmp_id] = lc
        self.cur_tmp_id -= 1

        return lc

    def update(self, tic_id, aperture, lightcurve_type, **data):
        """Updates a lightcurve with the given tic, aperture, and type.
        **data will apply keyword assignments to the lightcurve.

        Any updates will set the manager to track the target for updating.

        See the lightcurve model docs to see what fields can be assigned
        using keyword arguments

        Arguments
        ---------
        tic_id : int
            The TIC of the target you want to update
        aperture : str
            The ``Aperture.name`` of the target.
        lightcurve_type : str
            The ``LightcurveType.name`` of the target.
        """
        id_ = self.resolve_id(tic_id, aperture, lightcurve_type)
        checked_data = self.__validate__(tic_id, aperture, lightcurve_type, **data)
        self.update_w_id(id_, **checked_data)

    def update_w_id(self, id_, **data):
        """Updates a lightcurve with the given PSQL id.
        **data will apply assignments via keyword to the lightcurve.

        Any updates will set the manager to track the target for updating.

        See the lightcurve model docs to see what fields can be assigned
        using keyword arguments.

        Arguments
        ---------
        id : int
            The given PSQL integer for the lightcurve

        **data : Arbitrary keyword arguments
            Passed to ``Lightcurve`` for merging parameters.

        Returns
        -------
        ``Lightcurve``
            The updated lightcurve.
        """

        target_lc = self.id_map[id_]
        target_lc.merge_np(
            data['cadences'],
            data['bjd'],
            data['values'],
            data['errors'],
            data['x_centroids'],
            data['y_centroids'],
            data['quality_flags']
        )
        return target_lc

    def upsert(self, tic_id, aperture, lightcurve_type, **data):
        id_check = self.resolve_id(tic_id, aperture, lightcurve_type)

        if id_check:
            # Must update
            checked_data = self.__validate__(tic_id, aperture, lightcurve_type, **data)
            self.update_w_id(id_check, **checked_data)
        else:
            # Must insert
            self.add(tic_id, aperture, lightcurve_type, **data)

    def upsert_kwarg(self, **kwargs):
        tic_id = kwargs.pop('tic_id')
        aperture = kwargs.pop('aperture_id')
        lightcurve_type = kwargs.pop('lightcurve_type_id')
        self.upsert(tic_id, aperture, lightcurve_type, **kwargs)
        

    def resolve_to_db(self, db, resolve_conflicts=True):
        """
        Execute add and update statements to the database.

        Arguments
        ---------
        db : ``lightcurvedb.core.connection.DB``
            The given lightcurvedb Session Wrapper to mediate
            the connection to the database.
        resolve_conflicts : bool, optional
            If ``True`` (default), attempt to resolve unique
            constraint conflicts with the database.
        """
        if resolve_conflicts:
            # Determine if any of the lightcurves to be inserted need to
            # be merged, filter using defined tic ids in the manager
            q = db.lightcurve_id_map(
                [Lightcurve.tic_id.in_(self.tics)],
                resolve=False
            )
            id_mapper = pd.read_sql(
                q.statement,
                db.session.bind,
                index_col=['tic_id', 'aperture_id', 'lightcurve_type_id']
            )

            # Resolve insertions into updates if needed
            ids_to_remove = set()
            for id_, lightcurve in self.id_map.items():
                if id_ > 0:
                    continue
                # "New" lightcurve
                try:
                    _ = id_mapper.loc[
                        (
                            Lightcurve.tic_id,
                            Lightcurve.aperture_id,
                            Lightcurve.lightcurve_type_id
                        )
                    ]['id']
                    # This would have resulted in a unique-constraint
                    # collision. Perform update
                    defined_lightcurve = db.get_lightcurve(
                        lightcurve.tic_id,
                        lightcurve.aperture_id,
                        lightcurve.lightcurve_type_id
                    )
                    defined_lightcurve.merge(lightcurve)

                    # Remove refs to "add" lightcurve
                    ids_to_remove.add(id_)
                except KeyError:
                    # Lightcurve is indeed "new"
                    continue

            for id_ in ids_to_remove:
                del self.id_map[id_]

        ids_to_add = set(filter(lambda id_: id_ < 0, self.id_map.keys()))
        db.session.bulk_save_objects(
            [self.id_map[id_] for id_ in ids_to_add]
        )
