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
from lightcurvedb.models import Lightcurve, Lightpoint
from lightcurvedb.models.lightpoint import lightpoints_from_kw
from lightcurvedb.models.aperture import BestApertureMap
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


def set_dict():
    """Helper to create default dictionaries with set objects"""
    return defaultdict(set)


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
        return '<LightcurveManager: {} lightcurves>'.format(len(self))

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

    def __keyword_data_to_lightpoints__(self, **data):
        # Check for basic information that is needed
        if not 'cadences' in data or not 'bjd' in data:
            raise ValueError(
                "'cadences' and 'bjd' need to be specified in given data"
            )
        # Ensure that all data is of the same length
        cadences = data.pop('cadences')
        cadence_length = len(cadences)  # Use cadences as reference

        if any(len(col) != cadence_length for col in data):
            raise ValueError(
                ('Given data {} does not '
                 'match the length of the '
                 'given cadences {}'
                ).format(len(col), cadence_length)
            )

        # All data is aligned, assume user has provided everything in the
        # order of the given cadences
        return lightpoints_from_kw(
            **data
        )

    def add_defined_lightcurve(self, lightcurve):
        """
        Tracks a lightcurve that has a defined ID. If such a lightcurve were
        to contain identifiers that already exist within the manager then
        the appropriate id will be assigned to the manager.

        Arguments
        ---------
        lightcurve : ``Lightcurve``
            The lightcurve to add to the manager.

        Returns
        -------
        ``Lightcurve``
            The merged lightcurve as viewed by the manager.
        """
        self.add_model(lightcurve)

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
        lc = Lightcurve(
            tic_id=tic_id,
            aperture_id=aperture_id,
            lightcurve_type_id=lightcurve_type
        )
        self.add_model(lc)
        lc.lightpoints.extend(
            self.__keyword_data_to_lightpoints__(**data)
        )

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

        Returns
        -------
        Lightcurve
            Returns the updated lc model
        """
        lc = self.get_model(tic_id, aperture, lightcurve_type)
        lc.lightpoints.extend(
            self.__keyword_data_to_lightpoints__(
                **data
            )
        )

        return lc

    def upsert(self, tic_id, aperture, lightcurve_type, **data):
        """
        A complex method that attempts to find an existing lightcurve
        and update it with the provided ``**data`` interpreted as
        `Lightpoints`. If no such lightcurve could be found, one is
        instead instantiated and it's lightpoint data is constructed
        using the passed ``**data``.

        Parameters
        ----------
        tic_id : int
            The TIC identifier of the lightcurve.
        aperture : str
            The ``Aperture.name`` of the lightcurve. Must reference an
            existing Aperture.
        lightcurve_type : str
            The ``LightcurveType.name`` of the lightcurve. Must reference an
            existing LightcurveType.
        **data : keyword arguments of listlikes
            `Must` contain the keyword parameters of ``cadences`` and `bjd`.
            These keyword parameters must contain equal length lists that will
            be given to instantiate a list of Lightpoints.
        """

        try:
            lc = self.get_model(tic_id, aperture, lightcurve_type)
        except KeyError:
            lc = self.add_model_kw(
                tic_id=tic_id,
                aperture_id=aperture,
                lightcurve_type_id=lightcurve_type
            )

        lc.lightpoints.extend(
            self.__keyword_data_to_lightpoints__(
                **data
            )
        )
        return lc

    def upsert_kwarg(self, **kwargs):
        tic_id = kwargs.pop('tic_id')
        aperture = kwargs.pop('aperture_id')
        lightcurve_type = kwargs.pop('lightcurve_type_id')
        self.upsert(tic_id, aperture, lightcurve_type, **kwargs)
