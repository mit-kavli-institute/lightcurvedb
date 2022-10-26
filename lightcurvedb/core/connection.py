import os

import numpy as np
from pandas import read_sql as pd_read_sql
from sqlalchemy import and_, func
from sqlalchemy.orm import Session, sessionmaker

from lightcurvedb import models
from lightcurvedb.core.engines import thread_safe_engine
from lightcurvedb.core.psql_tables import PGCatalogMixin
from lightcurvedb.io.procedures import procedure
from lightcurvedb.models.best_lightcurve import BestOrbitLightcurveAPIMixin
from lightcurvedb.models.frame import FRAME_DTYPE, FrameAPIMixin
from lightcurvedb.models.lightcurve import ArrayOrbitLightcurveAPIMixin
from lightcurvedb.models.lightpoint import LIGHTPOINT_NP_DTYPES
from lightcurvedb.models.metrics import QLPMetricAPIMixin
from lightcurvedb.models.orbit import ORBIT_DTYPE, OrbitAPIMixin
from lightcurvedb.models.table_track import TableTrackerAPIMixin
from lightcurvedb.util.constants import __DEFAULT_PATH__
from lightcurvedb.util.type_check import isiterable

# Bring legacy capability
# TODO Encapsulate so it doesn't pollute this namespace
LEGACY_FRAME_TYPE_ID = "Raw FFI"
FRAME_COMP_DTYPE = [("orbit_id", np.int32)] + FRAME_DTYPE


class DB(
    Session,
    BestOrbitLightcurveAPIMixin,
    FrameAPIMixin,
    TableTrackerAPIMixin,
    OrbitAPIMixin,
    ArrayOrbitLightcurveAPIMixin,
    PGCatalogMixin,
    QLPMetricAPIMixin,
):
    """Wrapper for SQLAlchemy sessions. This is the primary way to interface
    with the lightcurve database.

    It is advised not to instantiate this class directly. The preferred
    methods are through

    ::

        from lightcurvedb import db
        with db as opendb:
            foo

        # or
        from lightcurvedb import db_from_config
        db = db_from_config('path_to_config')

    """

    @property
    def orbits(self):
        """
        A quick property that aliases ``db.query(Orbit)``.

        Returns
        -------
        sqlalchemy.Query
        """
        return self.query(models.Orbit)

    @property
    def apertures(self):
        """
        A quick property that aliases ``db.query(Aperture)``.

        Returns
        -------
        sqlalchemy.Query
        """
        return self.query(models.Aperture)

    @property
    def lightcurves(self):
        """
        A quick property that aliases ``db.query(Lightcurve)``.

        Returns
        -------
        sqlalchemy.Query
        """
        return self.query(models.Lightcurve)

    @property
    def lightcurve_types(self):
        """
        A quick property that aliases ``db.query(LightcurveType)``.

        Returns
        -------
        sqlalchemy.Query
        """
        return self.query(models.LightcurveType)

    # Begin orbit methods
    def query_orbits_by_id(self, orbit_numbers):
        """Grab a numpy array representing the orbits"""
        orbits = (
            self.query(*models.Orbit.get_legacy_attrs())
            .filter(models.Orbit.orbit_number.in_(orbit_numbers))
            .order_by(models.Orbit.orbit_number)
        )
        return np.array(list(map(tuple, orbits)), dtype=ORBIT_DTYPE)

    def query_orbit_cadence_limit(
        self,
        orbit_number,
        cadence_type,
        camera,
        frame_type=LEGACY_FRAME_TYPE_ID,
    ):
        """
        Returns the upper and lower cadence boundaries of an orbit. Since each
        orbit will have frames from multiple cameras a camera parameter is
        needed. In addition, TESS switched to 10 minute cadence numberings
        in July 2020.

        Parameters
        ----------
        orbit_number : int
            The orbit number.
        cadence_type : int
            The cadence type to consider for each frame.
        camera : int
            The camera number.
        frame_type : optional, str
            The frame type to consider. By default the type is of "Raw FFI".
            But this can be changed for any defined ``FrameType.name``.

        Returns
        -------
        tuple(int, int)
            A tuple representing the (min, max) cadence boundaries of the given
            parameters.
        """
        cadence_limit = (
            self.query(
                func.min(models.Frame.cadence), func.max(models.Frame.cadence)
            )
            .join(models.Orbit, models.Frame.orbit_id == models.Orbit.id)
            .filter(
                models.Frame.cadence_type == cadence_type,
                models.Frame.camera == camera,
                models.Frame.frame_type_id == frame_type,
                models.Orbit.orbit_number == orbit_number,
            )
        )

        return cadence_limit.one()

    def query_orbit_tjd_limit(self, orbit_id, cadence_type, camera):
        """
        Returns the upper and lower tjd boundaries of an orbit. Since each
        orbit will have frames from multiple cameras a camera parameter is
        needed. In addition, TESS switched to 10 minute cadence numberings
        in July 2020.

        Parameters
        ----------
        orbit_number : int
            The orbit number.
        cadence_type : int
            The cadence type to consider for each frame.
        camera : int
            The camera number.
        frame_type : optional, str
            The frame type to consider. By default the type is of "Raw FFI".
            But this can be changed for any defined ``FrameType.name``.

        Returns
        -------
        tuple(float, float)
            A tuple representing the (min, max) tjd boundaries of the given
            parameters.

        """
        tjd_limit = (
            self.query(
                func.min(models.Frame.start_tjd),
                func.max(models.Frame.end_tjd),
            )
            .join(models.Frame.orbit)
            .filter(
                models.Frame.cadence_type == cadence_type,
                models.Frame.camera == camera,
                models.Orbit.orbit_number == orbit_id,
            )
        )

        return tjd_limit.one()

    def query_frames_by_orbit(self, orbit_number, cadence_type, camera):
        """
        Determines the per-frame parameters of a given orbit, camera, and
        cadence type.

        Parameters
        ----------
        orbit_number : int
            The physical orbit number wanted.
        cadence_type : int
            The frame cadence type to consider. TESS switched to 10 minute
            cadences starting July 2020.
        camera : int
            Only frames recorded in this camera will be queried for.

        Returns
        -------
        np.ndarray
            See `Frame.get_legacy_attrs()` for a list of parameter names
            and their respective dtypes.
        """
        # Differs from PATools in that orbit_id != orbit number
        # so we need to record that.
        cols = [models.Orbit.orbit_number] + list(
            models.Frame.get_legacy_attrs()
        )
        values = (
            self.query(*cols)
            .join(models.Frame.orbit)
            .filter(
                models.Frame.cadence_type == cadence_type,
                models.Frame.camera == camera,
                models.Orbit.orbit_number == orbit_number,
            )
            .order_by(models.Frame.cadence.asc())
            .all()
        )

        return np.array(list(map(tuple, values)), dtype=FRAME_COMP_DTYPE)

    def query_frames_by_cadence(self, camera, cadence_type, cadences):
        """
        Determines the per-frame parameters of given cadences and
        by camera, and cadence type.

        Parameters
        ----------
        camera : int
            Only frames recorded in this camera will be queried for.
        cadence_type : int
            The frame cadence type to consider. TESS switched to 10 minute
            cadences starting July 2020.
        cadences : an iterable of integers
            Only frames with the provided cadences will be queries for.

        Returns
        -------
        np.ndarray
            See `Frame.get_legacy_attrs()` for a list of parameter names
            and their respective dtypes.

        """
        cols = [models.Orbit.orbit_number] + list(
            models.Frame.get_legacy_attrs()
        )
        values = (
            self.query(*cols)
            .join(models.Frame.orbit)
            .filter(
                models.Frame.cadence_type == cadence_type,
                models.Frame.camera == camera,
                models.Frame.cadence.in_(cadences),
            )
            .order_by(models.Frame.cadence.asc())
            .all()
        )

        return np.array(list(map(tuple, values)), dtype=FRAME_COMP_DTYPE)

    def query_all_orbit_ids(self):
        """
        A legacy method to return all orbit numbers in ascending
        order.

        Returns
        -------
        list
            A list of 1 element tuples containing ``(orbit_number,)``

        Note
        ----
        This is a legacy method. In this context ``orbit_id`` corresponds
        to the parameter ``Orbit.orbit_number`` and **not** ``Orbit.id``.
        """
        return (
            self.query(models.Orbit.orbit_number)
            .order_by(models.Orbit.orbit_number.asc())
            .all()
        )

    # Begin Lightcurve Methods
    def query_lightcurves(self, tics=None, apertures=None, types=None):
        """
        Make a query of lightcurves that meet the provided
        parameters. The emitted SQL clause is an IN statement for the given
        tics, apertures, and types (grouped using an AND).

        Arguments
        ---------
        tics : list, optional
            Filter lightcurves that have TIC identifiers contained in this
            list. If this list is empty then no filter will be applied using
            ``tics``.
        apertures : list, optional
            Filter lightcurves that have one of the given ``Aperture.name``
            strings. If this list is empty then no filter will be applied
            using ``apertures``.
        types : list, optional
            Filter lightcurves that have of of the given
            ``LightcurveType.name`` strings. If this list is empty then no
            filter will be applied using ``types``.

        Returns
        -------
        sqlalchemy.orm.query.Query
            A query of lightcurves that match the given parameters.
        """
        q = self.lightcurves
        if apertures is not None:
            q = q.filter(models.Lightcurve.aperture_id.in_(apertures))
        if types is not None:
            q = q.filter(models.Lightcurve.lightcurve_type_id.in_(types))
        if tics is not None:
            q = q.filter(models.Lightcurve.tic_id.in_(tics))
        return q

    def load_from_db(self, tics=None, apertures=None, types=None):
        """
        A quick method to return a list of lightcurves that meet the provided
        parameters. The emitted SQL clause is an IN statement for the given
        tics, apertures, and types (grouped using an AND).

        Arguments
        ---------
        tics : list, optional
            Filter lightcurves that have TIC identifiers contained in this
            list. If this list is empty then no filter will be applied using
            ``tics``.
        apertures : list, optional
            Filter lightcurves that have one of the given ``Aperture.name``
            strings. If this list is empty then no filter will be applied
            using ``apertures``.
        types : list, optional
            Filter lightcurves that have of of the given
            ``LightcurveType.name`` strings. If this list is empty then no
            filter will be applied using ``types``.

        Returns
        -------
        list
            A list of lightcurves that match the given parameters. This list
            will be empty if no lightcurves match.

        """
        q = self.query_lightcurves(tics=tics, apertures=apertures, types=types)
        return q.all()

    def yield_from_db(self, chunksize, tics=None, apertures=None, types=None):
        """
        Akin to ``load_from_db`` but instead of a list we return an iterator
        over a `server-side` PSQL cursor. This may be beneficial when
        interacting with hundreds of thousands of lightcurves or you don't
        need to load everything into memory at once.

        Arguments
        ---------
        chunksize : int
            The number of lightcurves the `PSQL` cursor should return as
            it executes the query. *NOTE* this is **NOT** the number of
            lightcurves returned each time the iterator yields. The
            iterator will return 1 lightcurve at a time.
        tics : list, optional
            Filter lightcurves that have TIC identifiers contained in this
            list. If this list is empty then no filter will be applied.
        apertures : list, optional
            Filter lightcurves that have one of the given ``Aperture.name``
            strings. If this list is empty then no filter will be applied
            using ``apertures``.
        types : list, optional
            Filter lightcurves that have of of the given
            ``LightcurveType.name`` strings. If this list is empty then no
            filter will be applied using ``types``.

        Returns
        -------
        iter
            An iterator over the given query that returns 1 row (lightcurve)
            at a time.

        Notes
        -----
        Server-side cursors are not free and have higher memory requirements
        for fileservers given certain circumstances than `normal` queries.
        This method is best reserved for very large queries.
        """
        q = self.query_lightcurves(tics=tics, apertures=apertures, types=types)
        return q.yield_per(chunksize)

    def get_lightcurve(self, tic, lightcurve_type, aperture, resolve=True):
        """
        Retrieves a single lightcurve row.

        Arguments
        ---------
        tic : int
            The TIC identifier of the lightcurve.
        aperture : str
            The aperture name of the lightcurve.
        lightcurve_type : str
            The name of the lightcurve's type.
        resolve : bool, optional
            If True return a single Lightcurve object, or a query instance.

        Returns
        -------
        Lightcurve or sqlalchemy.orm.query.Query
            Returns either a single Lightcurve instance or a Query object.

        Raises
        ------
        sqlalchemy.orm.exc.NoResultFound
            No lightcurve matched your requirements.

        """
        q = self.lightcurves.filter(
            models.Lightcurve.tic_id == tic,
            models.Lightcurve.aperture_id == aperture,
            models.Lightcurve.lightcurve_type_id == lightcurve_type,
        )

        if resolve:
            return q.one()
        return q

    def lightcurves_from_tics(self, tics, **kw_filters):
        """
        Retrieves lightcurves from a collection of TIC identifiers.
        Can also apply keyword filters.

        .. deprecated:: 0.9.0
            ``kw_filters`` will be replaced instead with ``resolve`` to
            allow filters that span relationships.

        Arguments
        ---------
        tics : list or collection of integers
            The set of tics to filter for.
        **kw_filters : Keyword arguments, optional
            Keyword arguments to pass into ``filter_by``.
        """
        q = self.lightcurves.filter(
            models.Lightcurve.tic_id.in_(tics)
        ).filter_by(**kw_filters)
        return q

    def tics_by_orbit(
        self,
        orbit_numbers,
        cameras=None,
        ccds=None,
        resolve=True,
        unique=True,
        sort=True,
    ):
        """
        Return tics by observed in the given orbit numbers. This query can be
        filtered for specific cameras/ccds.

        Arguments
        ---------
        orbit_numbers : int or iterable of integers
            The orbit numbers to discriminate for.
        cameras : list of integers, optional
            A list of cameras to discriminate for. If None (default) then no
            cameras will be filtered for.
        ccds : list of integers, optional
            A list of ccds to discriminate for. If None (default) then no
            ccds will be filtered for.
        resolve : bool, optional
            If True, resolve the query into a list of integers. If False
            return the ``sqlalchemy.orm.Query`` object representing
            the intended SQL statement.
        unique : bool, optional
            Since the same TIC can appear across multiple observations
            and JOIN statements produce cross products of Tables, query
            results might contain duplicate TICs. Setting ``unique`` to
            ``True`` will make the return be a proper set of TICs.
        sort : bool, optional
            If ``True`` apply an Ascending sort to the return.

        Returns
        -------
        list of integers or ``sqlalchemy.orm.Query``
            Returns either the result of the query or the Query object itself.
        """
        if isinstance(orbit_numbers, str):
            orbit_numbers = [int(orbit_numbers)]

        if not isiterable(orbit_numbers):
            orbit_numbers = [orbit_numbers]

        col = models.Lightcurve.tic_id

        if unique:
            col = col.distinct()

        q = (
            self.query(col)
            .join(models.Lightcurve.observations)
            .join(models.Observation.orbit)
            .filter(models.Orbit.orbit_number.in_(orbit_numbers))
        )

        if cameras:
            q = q.filter(models.Observation.camera.in_(cameras))
        if ccds:
            q = q.filter(models.Observation.ccd.in_(ccds))

        if sort:
            q = q.order_by(models.Lightcurve.tic_id.asc())

        if resolve:
            return [r for r, in q.all()]
        return q

    def tics_by_sector(
        self,
        sectors,
        cameras=None,
        ccds=None,
        resolve=True,
        unique=True,
        sort=True,
    ):
        """
        Return tics by observed in the given sector numbers. This query can be
        filtered for specific cameras/ccds.

        Arguments
        ---------
        sectors : int or iterable of integers
            The sectors to discriminate for.
        cameras : list of integers, optional
            A list of cameras to discriminate for. If None (default) then no
            cameras will be filtered for.
        ccds : list of integers, optional
            A list of ccds to discriminate for. If None (default) then no
            ccds will be filtered for.
        resolve : bool, optional
            If True, resolve the query into a list of integers. If False
            return the ``sqlalchemy.orm.Query`` object representing
            the intended SQL statement.
        unique : bool, optional
            Since the same TIC can appear across multiple observations
            and JOIN statements produce cross products of Tables, query
            results might contain duplicate TICs. Setting ``unique`` to
            ``True`` will make the return be a proper set of TICs.
        sort : bool, option
            If ``True`` apply an Ascending sort to the return.

        Returns
        -------
        list of integers or ``sqlalchemy.orm.Query``
            Returns either the result of the query or the Query object itself.
        """

        if isinstance(sectors, str):
            sectors = [int(sectors)]

        if not isiterable(sectors):
            sectors = [sectors]

        col = models.Lightcurve.tic_id
        if unique:
            col = col.distinct()

        q = (
            self.query(col)
            .join(models.Lightcurve.observations)
            .join(models.Observation.orbit)
            .filter(models.Orbit.sector.in_(sectors))
        )

        if cameras:
            q = q.filter(models.Observation.camera.in_(cameras))
        if ccds:
            q = q.filter(models.Observation.ccd.in_(ccds))

        if sort:
            q = q.order_by(models.Lightcurve.tic_id.asc())

        if resolve:
            return [r for r, in q.all()]
        return q

    def lightcurves_by_orbit(
        self, orbit_numbers, cameras=None, ccds=None, resolve=True
    ):
        """
        Retrieve lightcurves that have been observed in the given
        orbit numbers. This method can also filter by camera and ccd.

        Arguments
        ---------
        orbit_numbers : list of integers
            The orbits to filter on.
        cameras : list of integers, optional
            List of cameras to query against. If None, then don't
            discriminate using cameras.
        ccds : list of integers, optional
            List of ccds to query against. If None, then don't discriminate
            using ccds
        resolve : bool, optional
            If True, resolve the query into a list of Lightcurves. If False
            return the ``sqlalchemy.orm.Query`` object representing
            the intended SQL statement.

        Returns
        -------
        list of lightcurves or ``sqlalchemy.orm.Query``
            Returns either the result of the query or the Query object itself.
        """

        q = self.lightcurves.join(models.Lightcurve.observations).join(
            models.Observation.orbit
        )

        if isinstance(orbit_numbers, int):
            q = q.filter(models.Orbit.orbit_number == orbit_numbers)
        else:
            q = q.filter(models.Orbit.orbit_number.in_(orbit_numbers))

        if cameras:
            q = q.filter(models.Observation.camera.in_(cameras))
        if ccds:
            q = q.filter(models.Observation.ccd.in_(ccds))

        if resolve:
            return q.all()
        return q

    def lightcurves_from_best_aperture(self, q=None, resolve=True):
        """
        Find Lightcurve rows based upon their best aperture.

        Arguments
        ---------
        q : sqlalchemy.orm.query.Query, optional
            An initial Lightcurve Query. If left to None then all lightcurves
            will be queried for.
        resolve : bool, optional
            If True execute the Query into a list of Lightcurves. If False,
            return a Query object.

        Returns
        -------
        list of ``Lightcurves`` or a ``sqlalchemy.orm.query.Query``


        Notes
        -----
        This methods finds Lightcurves by JOINing them onto the BestAperture
        table. SQL JOINs find the cartesian product of two tables. This
        product is filtered by ``Lightcurve.tic_id == BestApertureMap.tic_id``
        and ``Lightcurve.aperture_id == BestApertureMap.aperture_id``. In this
        way the catesian product is filtered and the expected Best Aperture
        filter is achieved.

        For best results additional filters should be applied as this JOIN
        will, by default, attempt to find the cartesian product of the
        entire lightcurve and best aperture tables.

        This can be done by first passing in a ``sqlalchemy.orm.query.Query``
        object as the ``q`` parameter. This query must be on the
        ``Lightcurve`` table.

        ::

            # Example
            q = db.lightcurves_by_orbit(23, resolve=False) # Get init. query

            lcs = db.lightcurves_from_best_apertures(q=q)

            # Retrives lcs that appear in orbit 23 and filtered
            # for best aperture.
        """
        bestap_tic_id = models.BestApertureMap.tic_id
        bestap_aperture_id = models.BestApertureMap.aperture_id

        and_clause = and_(
            models.Lightcurve.tic_id == bestap_tic_id,
            models.Lightcurve.aperture_id == bestap_aperture_id,
        )

        if q is None:
            q = self.lightcurves

        q = q.join(models.BestApertureMap, and_clause)
        if resolve:
            return q.all()
        return q

    def lightcurve_id_map(self, filters=None, resolve=True):
        if not filters:
            filters = []

        q = self.query(
            models.Lightcurve.id,
            models.Lightcurve.tic_id,
            models.Lightcurve.aperture_id,
            models.Lightcurve.lightcurve_type_id,
        ).filter(*filters)

        if resolve:
            return q.all()
        return q

    def set_best_aperture(self, tic_id, aperture):
        """
        Maps the best aperture to a TIC id.

        Arguments
        ---------
        tic_id : int
            The TIC id to assign an Aperture
        aperture : str or Aperture
            The name of an Aperture or an Aperture instance

        Notes
        -----
        These changes are not committed. The user will need to call
        ``db.commit()`` in order for the changes to be made
        permanent.
        """
        upsert = models.BestApertureMap.set_best_aperture(tic_id, aperture)
        self.execute(upsert)

    def unset_best_aperture(self, tic_id):
        """
        Unsets the best aperture.

        Arguments
        ---------
        tic_id : int
            The TIC id to unassign

        Notes
        -----
        If there is no best aperture map for the given TIC id then no
        operation will take place. Any changes will require ``db.commit()``
        to be made permanent.
        """
        check = (
            self.query(models.BestApertureMap)
            .filter(models.BestApertureMap.tic_id == tic_id)
            .one_or_none()
        )
        if check:
            check.delete()

    # Begin helper methods to quickly grab reference maps
    @property
    def observation_df(self):
        """
        Return an observation dataframe for client-side lookups. Best used to
        avoid large amounts of rapid hits to the server-side database.

        Returns
        -------
        pd.DataFrame
            A dataframe of ``tic_id``, the ``camera``, ``ccd``, and
            ``orbit.orbit_number`` that the tic was observed in.
        """
        q = (
            self.query(
                models.Observation.tic_id,
                models.Observation.camera,
                models.Observation.ccd,
                models.Orbit.orbit_number,
            )
            .join(models.Observation.orbit)
            .order_by(
                models.Orbit.orbit_number.asc(),
                models.Observation.tic_id.asc(),
            )
        )
        return pd_read_sql(q.statement, self.bind)

    def get_partitions_df(self, model):
        """
        Attempt to return a pd.DataFrame on any partitions found
        on the given model.

        Parameters
        ----------
        model : QLPModel Class
            The model to search for partitions.

        Returns
        -------
        pd.DataFrame
        """
        return model.partition_df(self)

    def get_cadences_in_orbits(self, orbits):
        """
        Get the defined cadences observed in the given orbits.

        Parameters
        ----------
        orbits : iterable of int
            The orbit numbers to search for cadences.
        """
        q = (
            self.query(models.Frame.cadence)
            .join(models.Frame.orbit)
            .filter(models.Orbit.orbit_number.in_(orbits))
            .distinct(models.Frame.cadence)
            .order_by(models.Frame.cadence.asc())
        )
        return [r for r, in q.all()]

    def cadences_in_sectors(self, sectors, frame_type="Raw FFI", resolve=True):
        q = (
            self.query(models.Frame.cadence)
            .filter(
                models.Orbit.sector.in_(sectors),
                models.Frame.frame_type_id == frame_type,
            )
            .distinct(models.Frame.cadence)
        )

        return [r for r, in q] if resolve else q

    def cadences_in_orbit(
        self, orbit_numbers, frame_type="Raw FFI", resolve=True
    ):
        q = (
            self.query(models.Frame.cadence)
            .filter(
                models.Orbit.orbit_number.in_(orbit_numbers),
                models.Frame.frame_type_id == frame_type,
            )
            .distinct(models.Frame.cadence)
        )

        return [r for r, in q] if resolve else q

    def get_baked_lcs(self, ids):
        return (
            self.query(
                models.Lightpoint.lightcurve_id,
                models.Lightpoint.ordered_column("cadence").label("cadences"),
                models.Lightpoint.ordered_column("bjd").label("bjd"),
                models.Lightpoint.ordered_column("data").label("values"),
                models.Lightpoint.ordered_column("quality_flag").label(
                    "quality_flags"
                ),
            )
            .join(models.Lightpoint.lightcurve)
            .filter(models.Lightpoint.lightcurve_id.in_(ids))
            .group_by(models.Lightpoint.lightcurve_id)
        )

    def get_best_aperture_data(self, tic_id, *columns):
        """
        Build a structured numpy array with the best aperture lightcurve
        data associated with the given TIC id. Columns can also be provided
        if one does not want the full representation of the lightcurve.

        Parameters
        ----------
        tic_id : int
            The TIC id to find lightcurve data for. This entry must
            have defined lightcurves as well as an associated bestaperture
            entry.
        *columns : variadic str, optional
            If empty all columns of Lightpoint will be returned. Otherwise,
            one may specify a subset of Lightpoint columns. These names
            must appear as they do on the Lightpoint model.
        Returns
        ------
        A structured numpy.ndarray with Lightpoint fieldnames as the
        field keys.

        Raises
        ------
        InternalError:
            No data was found for this TIC id.
        """
        if not columns:
            columns = models.Lightpoint.get_columns()

        stmt = procedure.get_bestaperture_data(tic_id, *columns)
        return np.array(
            list(map(tuple, self.execute(stmt))),
            dtype=[
                (column, LIGHTPOINT_NP_DTYPES[column]) for column in columns
            ],
        )


def db_from_config(config_path=None, db_class=None, **engine_kwargs):
    """
    Create a DB instance from a configuration file.

    Arguments
    ---------
    config_path : str or Path, optional
        The path to the configuration file.
        Defaults to ``~/.config/lightcurvedb/db.conf``. This is expanded
        from the user's ``~`` space using ``os.path.expanduser``.
    **engine_kwargs : keyword arguments, optional
        Arguments to pass off into engine construction.
    """
    engine = thread_safe_engine(
        os.path.expanduser(config_path if config_path else __DEFAULT_PATH__),
        **engine_kwargs,
    )

    db_class = DB if db_class is None else db_class

    factory = sessionmaker(bind=engine, class_=db_class)
    return factory()


# Try and instantiate "global" lcdb
try:
    db = db_from_config()
except KeyError:
    db = None
