from collections import defaultdict
from functools import partial
import os
import multiprocessing as mp
from itertools import groupby
from sqlalchemy.ext.serializer import dumps
from sqlalchemy.sql.expression import bindparam
from sqlalchemy.dialects.postgresql import insert
from lightcurvedb.exceptions import LightcurveDBException
from lightcurvedb.models import Lightcurve


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

    def __init__(self, lightcurves, internal_session=None):
        """__init__.

        Parameters
        ----------
        lightcurves :
            An iterable collection of lightcurves to manage.
        """
        self.tics = set_dict()
        self.apertures = set_dict()
        self.types = set_dict()
        self.id_map = dict()

        self.aperture_defs = {}
        self.type_defs = {}

        self._to_add = list()
        self._to_update = list()
        self._to_upsert = list()

        if internal_session:
            self.aperture_defs = {
                ap.name: ap for ap in internal_session.apertures.all()
            }
            self.type_defs = {
                t.name: t for t in internal_session.lightcurve_types.all()
            }
        self.internal_session = internal_session


        for lightcurve in lightcurves:
            self.tics[lightcurve.tic_id].add(lightcurve.id)
            self.apertures[lightcurve.aperture.name].add(lightcurve.id)
            self.types[lightcurve.lightcurve_type.name].add(lightcurve.id)
            self.id_map[lightcurve.id] = lightcurve

            self.aperture_defs[lightcurve.aperture.name] = lightcurve.aperture
            self.type_defs[lightcurve.lightcurve_type.name] = lightcurve.lightcurve_type

        self.searchables = (
            self.tics,
            self.apertures,
            self.types
        )

    def __repr__(self):
        return '<LightcurveManager: {} lightcurves>'.format(len(self))

    def __getitem__(self, key):
        """__getitem__.

        Parameters
        ----------
        key :
            The key to search for
        Raises
        ------
        KeyError
            If the key is not found within the LightcurveManager
        """
        for searchable in self.searchables:
            if key in searchable:
                ids = searchable[key]
                if len(ids) == 1:
                    # Singular, just return the lightcurve
                    id = next(iter(ids))
                    return self.id_map[id]
                return LightcurveManager([self.id_map[id_] for id_ in ids])

        raise KeyError(
            'The keyword \'{}\' was not found in the query'.format(key)
        )

    def __len__(self):
        """__len__.
        The length of the manager in terms of number of stored lightcurves.
        """
        return len(self.id_map)

    def __iter__(self):
        """__iter__.
        Iterate over the stored lightcurves.
        """
        return iter(self.id_map.values())


    def __validate__(self, **data):
        lengths = map(len, (data[attr] for attr in self.array_attrs if attr in data))
        length_0 = next(lengths)
        if not all(l == length_0 for l in lengths):
            raise IncongruentLightcurve(
                'Lightcurve is being improperly modified with array lengths {}'.format(lengths)
            )

    def resolve_id(self, tic_id, aperture, lightcurve_type):
        lc_by_tics = self.tics.get(tic_id, set())
        if isinstance(aperture, str):
            lc_by_aps = self.apertures.get(aperture, set())
        else:
            lc_by_aps = self.apertures.get(aperture.name, set())
        if isinstance(lightcurve_type, str):
            lc_by_types = self.types.get(lightcurve_type, set())
        else:
            lc_by_types = self.types.get(lightcurve_type.name, set())

        try:
            return set.union(lc_by_tics, lc_by_aps, lc_by_types).pop()
        except KeyError:
            # Nothing to resolve
            return None

    def clear_tracked(self):
        self._to_add = list()
        self._to_update = list()
        self._to_upsert = list()

    def add(self, tic_id, aperture, lightcurve_type, **data):
        """Adds a new lightcurve to the manager. This will create a new
        Lightcurve model instance and track it for batch insertions.

        Arguments:
            tic_id {int} -- The TIC Number for the new Lightcurve
            aperture {str, Aperture} -- The aperture to be linked
            lightcurve_type {str, LightcurveType} -- The type of lightcurve

        Raises:
            DuplicateEntryException: Raised when attempting to add a
            lightcurve that already contains the same tic, aperture, and type
            in order to avoid a PSQL Unique Contraint violation that will
            invalidate mass queries. Caveat: will only catch unique constraint
            violations within this Manager instance's context.
        """
        try:
            assert self.resolve_id(tic_id, aperture, lightcurve_type) is None
        except AssertionError:
            raise DuplicateEntryException(
                '{} already exists in the manager'.format(
                    (tic_id, aperture, lightcurve_type)
                )
             )
        self.__validate__(**data)

        # Past this point we are guaranteed a unique lightcurve (in the
        # context of the current manager context)
        new_lc = {
            'tic_id': tic_id,
            'aperture_id': self.aperture_defs[str(aperture)].id,
            'lightcurve_type_id': self.type_defs[str(lightcurve_type)].id,
            'cadence_type': 30,
        }
        new_lc.update(data)
        self._to_add.append(new_lc)


    def update(self, tic_id, aperture, lightcurve_type, **data):
        """Updates a lightcurve with the given tic, aperture, and type.
        **data will apply keyword assignments to the lightcurve.

        Any updates will set the manager to track the target for updating.

        See the lightcurve model docs to see what fields can be assigned
        using keyword arguments

        Arguments:
            tic_id {int} -- The TIC of the target you want to update
            aperture {str, Aperture} -- The aperture of the target
            lightcurve_type {str, LightcurveType} -- The lightcurve type of
                the target
        """
        lc_to_find = self.resolve_id(tic_id, aperture, lightcurve_type)
        self.update_w_id(lc_to_find, **data)

    def update_w_id(self, id, **data):
        """Updates a lightcurve with the given PSQL id.
        **data will apply assignments via keyword to the lightcurve.

        Any updates will set the manager to track the target for updating.

        See the lightcurve model docs to see what fields can be assigned
        using keyword arguments.

        Arguments:
            id {int} -- The given PSQL integer for the lightcurve

        Returns:
            Lightcurve -- The updated lightcurve
        """

        self.__validate__(**data)
        params = {'_id': id}
        params.update(data)
        self._to_update.append(params)

    def upsert(self, tic_id, aperture, lightcurve_type, **data):

        self.__validate__(**data)
        values = {}
        values['tic_id'] = tic_id
        values['aperture_id'] = self.aperture_defs[str(aperture)].id
        values['lightcurve_type_id'] = self.type_defs[str(lightcurve_type)].id
        values.update(data)
        self._to_upsert.append(values)

    def update_q(
        self,
        id_bind='_id',
        cadences='cadences',
        barycentric_julian_date='bjd',
        values='values',
        errors='errors',
        x_centroids='x_centroids',
        y_centroids='y_centroids',
        quality_flags='quality_flags'):

        mappings = Lightcurve.create_mappings(
            cadences=cadences,
            barycentric_julian_date=barycentric_julian_date,
            values=values,
            errors=errors,
            x_centroids=x_centroids,
            y_centroids=y_centroids,
            quality_flags=quality_flags
        )

        q = Lightcurve.__table__.update()\
            .where(
                Lightcurve.id == bindparam(id_bind)
            ).values(mappings)
        raise NotImplementedError

    def insert_q(
        self,
        id_bind='_id',
        cadences='cadences',
        barycentric_julian_date='bjd',
        values='values',
        errors='errors',
        x_centroids='x_centroids',
        y_centroids='y_centroids',
        quality_flags='quality_flags'):

        mappings = Lightcurve.create_mappings(
            cadences=cadences,
            barycentric_julian_date=barycentric_julian_date,
            values=values,
            errors=errors,
            x_centroids=x_centroids,
            y_centroids=y_centroids,
            quality_flags=quality_flags
        )

        q = Lightcurve.__table__.insert().values(mappings)
        return q

    def upsert_q(
        self,
        aperture_bind='aperture',
        lightcurve_type_bind='lightcurve_type',
        tic_id_bind='tic_id',
        cadences='cadences',
        barycentric_julian_date='bjd',
        values='values',
        errors='errors',
        x_centroids='x_centroids',
        y_centroids='y_centroids',
        quality_flags='quality_flags'):


        mappings = Lightcurve.create_mappings(
            cadences=cadences,
            barycentric_julian_date=barycentric_julian_date,
            values=values,
            errors=errors,
            x_centroids=x_centroids,
            y_centroids=y_centroids,
            quality_flags=quality_flags
        )

        q = insert(Lightcurve.__table__)\
            .values(
                mappings
            ).on_conflict_do_update(
                constraint=Lightcurve.__table_args__[0],
                set_=mappings
            )
        return q


    def execute(self, session=None):
        insert_q = self.insert_q()
        upsert_q = self.upsert_q()

        if session is None:
            session = self.internal_session

        if len(self._to_add) > 0:
            session.session.execute(
                insert_q, self._to_add
            )

        if len(self._to_update) > 0:
            # We need to group alike parameters and perform separate updates
            groups = groupby(self._to_update, lambda param: set(param.keys()))

            for params, values in groups:
                # Create a binding
                mapping = {}
                for param in params:
                    if param == 'bjd':
                        mapping['barycentric_julian_date'] = bindparam('bjd')
                    elif param == '_id':
                        continue
                    else:
                        mapping[param] = bindparam(param)
                q = Lightcurve.__table__.update().where(
                    Lightcurve.id == bindparam('_id')
                    ).values(mapping)
                session.session.execute(q, list(values))

        if len(self._to_upsert) > 0:
            session.session.execute(
                upsert_q, self._to_upsert
            )

        self.clear_tracked()
