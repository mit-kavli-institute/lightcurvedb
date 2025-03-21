import typing

import numpy as np
import pandas as pd
import pyticdb
import sqlalchemy as sa

from lightcurvedb import exceptions
from lightcurvedb import models as m
from lightcurvedb.core import psql_tables


class APIMixin:
    def model_query_to_numpy(self, q, dtype):
        raise NotImplementedError


class ApertureAPIMixin(APIMixin):
    def get_aperture_by_name(self, name: str):
        q = sa.select(m.Aperture).where(m.Aperture.name == name)
        return self.execute(q).fetchone()[0]


class BLSAPIMixin(APIMixin):
    def get_or_create_bls_tags(self, *tags):
        existing_tags_q = sa.select(m.BLSTag).where(m.BLSTag.name.in_(tags))
        existing_tags = {t.name: t for t in self.scalars(existing_tags_q)}
        for tag in tags:
            if tag not in existing_tags:
                new_tag = m.BLSTag(name=tag)
                self.add(new_tag)
                existing_tags[tag] = new_tag
        self.commit()
        return existing_tags


class LightcurveTypeAPIMixin(APIMixin):
    def get_lightcurve_type_by_name(self, name: str):
        q = sa.select(m.LightcurveType).where(m.LightcurveType.name == name)
        return self.execute(q).fetchone()[0]


class FrameAPIMixin(APIMixin):
    def get_mid_tjd_mapping(self, frame_type: typing.Union[str, None] = None):
        cameras = sa.select(m.Frame.camera.distinct())

        tjd_q = (
            sa.select(m.Frame.cadence, m.Frame.mid_tjd)
            .order_by(m.Frame.cadence)
        )

        if frame_type is not None:
            tjd_q = tjd_q.join(m.Frame.frame_type).where(m.FrameType.name == frame_type)

        mapping = {}
        for (camera,) in self.execute(cameras):
            q = tjd_q.where(m.Frame.camera == camera)
            df = pd.DataFrame(
                self.execute(q),
                columns=["cadence", "mid_tjd"],
                index=["cadence"],
            )
            mapping[camera] = df

        return mapping


class OrbitAPIMixin(APIMixin):
    def get_orbit_id(self, orbit_number: int) -> int:
        q = sa.select(m.Orbit.id).where(m.Orbit.orbit_number == orbit_number)
        return self.execute(q).fetchone()[0]

    def get_orbit_mapping(self, ident_col, *data_cols):
        q = sa.select(
            getattr(m.Orbit, ident_col),
            *[getattr(m.Orbit, col) for col in data_cols],
        )
        mapping = {}
        for ident, *data in self.execute(q):
            if len(data) == 1:
                data = data[0]
            mapping[ident] = data
        return mapping

    def query_orbit_cadence_limit(
        self,
        orbit_number: int,
        camera: int,
        frame_type: typing.Optional[str] = None,
        ccd: typing.Optional[int] = None,
    ):
        """
        Returns the upper and lower cadence boundaries of an orbit. Since each
        orbit will have frames from multiple cameras a camera parameter is
        needed.
        Parameters
        ----------
        orbit_number : int
            The orbit number.
        camera : int
            The camera number.
        frame_type : optional, str
            The frame type to consider.
        ccd : optional, int
            The CCD number.
        """
        q = (
            sa.select(
                sa.func.min(m.Frame.cadence).label("min_cadence"),
                sa.func.max(m.Frame.cadence).label("max_cadence"),
            )
            .join(m.Frame.orbit)
            .where(
                m.Frame.camera == camera,
                m.Orbit.orbit_number == orbit_number,
            )
        )
        if frame_type is not None:
            q = q.join(m.Frame.frame_type).where(m.FrameType.name == frame_type)
        if ccd is not None:
            q = q.where(m.Frame.ccd == ccd)
        return self.execute(q).fetchone()

    def query_orbit_tjd_limit(
        self,
        orbit_number: int,
        camera: int,
        frame_type: typing.Optional[str] = None,
        ccd: typing.Optional[int] = None,
    ):
        """
        Returns the upper and lower tjd boundaries of an orbit. Since each
        orbit will have frames from multiple cameras a camera parameter is
        needed. In addition, TESS switched to 10 minute cadence numberings
        in July 2020.

        Parameters
        ----------
        orbit_number : int
            The orbit number.
        camera : int
            The camera number.
        frame_type : optional, str
            The frame type to consider.
        ccd : optional, int
            The ccd number.

        Returns
        -------
        tuple(float, float)
            A tuple representing the (min, max) tjd boundaries of the given
            parameters.

        """

        q = (
            sa.select(
                sa.func.min(m.Frame.start_tjd).label("min_tjd"),
                sa.func.max(m.Frame.end_tjd).label("max_tjd"),
            )
            .join(m.Frame.orbit)
            .where(
                m.Frame.camera == camera,
                m.Orbit.orbit_number == orbit_number,
            )
        )
        if frame_type is not None:
            q = q.join(m.Frame.frame_type).where(m.FrameType.name == frame_type)
        if ccd is not None:
            q = q.where(m.Frame.ccd == ccd)
        return self.execute(q).fetchone()


class ArrayOrbitLightcurveAPIMixin(APIMixin):
    def _process_lc_selection(self, select_q):
        """
        Process a lightcurve query statement into a numpy structured
        array.

        Raises
        ------
        lightcurvedb.exceptions.EmptyLightcurve
            If the query results in an empty array, this exception will
            be raised with the statement executed.
        """
        structs = []
        stellar_param_info = {}
        for lc in self.execute(select_q).scalars():
            try:
                tmag = stellar_param_info[lc.tic_id]
            except KeyError:
                tmag = pyticdb.query_by_id(lc.tic_id, "tmag")[0]
                stellar_param_info[lc.tic_id] = tmag
            structs.append(lc.to_numpy(normalize=True, offset=tmag))

        if len(structs) == 0:
            raise exceptions.EmptyLightcurve(q=select_q)

        return np.concatenate(structs)

    def get_lightcurve(
        self, tic_id, lightcurve_type, aperture, orbit, resolve=True
    ):
        """
        Retrieves a single lightcurve row.

        Arguments
        ---------
        tic_id: int
            The TIC id of the desired lightcurve.
        aperture: str or integer
            The aperture name or aperture id of the desired lightcurve.
        lightcurve_type: str or integer
            The type name or type id of the desired lightcurve.
        orbit: integer
            The physical orbit number of the desired lightcurve.
        resolve: bool, optional
            If True, return the resolved lightcurve query, otherwise return
            the query object itself.

        Returns
        -------
        OrbitLightcurve, sqlalchemy.Query

        Raises
        ------
        sqlalchemy.orm.exc.NoResultFound
            No lightcurve matched the desired parameters.
        """

        q = (
            sa.select(m.ArrayOrbitLightcurve)
            .join(m.ArrayOrbitLightcurve.orbit)
            .where(m.Orbit.orbit_number == orbit)
        )

        if isinstance(aperture, str):
            q = q.join(m.ArrayOrbitLightcurve.aperture)
            q = q.where(m.ArrayOrbitLightcurve.aperture_name == aperture)
        else:
            q = q.where(m.ArrayOrbitLightcurve.aperture_id == aperture)

        if isinstance(lightcurve_type, str):
            q = q.join(m.ArrayOrbitLightcurve.lightcurve_type)
            q = q.where(
                m.ArrayOrbitLightcurve.lightcurve_type_name == lightcurve_type
            )
        else:
            q = q.where(
                m.ArrayOrbitLightcurve.lightcurve_type_id == lightcurve_type
            )

        if resolve:
            return self._process_lc_selection(q)

        return q

    def get_multisector_lightcurve(self, tic_id, lightcurve_type, aperture):
        q = (
            sa.select(m.ArrayOrbitLightcurve)
            .join(m.ArrayOrbitLightcurve.aperture)
            .join(m.ArrayOrbitLightcurve.lightcurve_type)
            .join(m.ArrayOrbitLightcurve.orbit)
            .where(
                m.ArrayOrbitLightcurve.tic_id == tic_id,
                m.Aperture.name == aperture,
                m.LightcurveType.name == lightcurve_type,
            )
            .order_by(m.Orbit.orbit_number)
        )
        return self._process_lc_selection(q)


class BestOrbitLightcurveAPIMixin(APIMixin):
    def get_best_aperture_assignment(self, tic_id, orbit_number):
        """
        Return the best aperture assignment for the given tic id and
        orbit_number.

        Parameters
        ----------
        tic_id: int
            The TIC identifier desired.
        orbit_number: int
            The Orbit number desired.

        Returns
        -------
        lightcurvedb.models.Aperture
        """
        q = (
            sa.select(m.Aperture)
            .join(m.BestOrbitLightcurve.aperture)
            .join(m.BestOrbitLightcurve.orbit)
            .where(
                m.BestOrbitLightcurve.tic_id == tic_id,
                m.Orbit.orbit_number == orbit_number,
            )
        )
        aperture = self.execute(q).fetchone()
        return aperture

    def get_best_lightcurve_type_assignment(self, tic_id, orbit_number):
        """
        Return the best lightcurve type assignment for the given tic id and
        orbit_number.

        Parameters
        ----------
        tic_id: int
            The TIC identifier desired.
        orbit_number: int
            The Orbit number desired.

        Returns
        -------
        lightcurvedb.models.LightcurveType
        """

        q = sa.select(m.LightcurveType).where(
            m.BestOrbitLightcurve.tic_id == tic_id,
            m.Orbit.orbit_number == orbit_number,
        )
        type = self.execute(q).fetchone()
        return type

    def get_multisector_best_lightcurve(
        self, tic_id, aperture=None, lightcurve_type=None
    ):
        BEST_LC = m.BestOrbitLightcurve
        LC = m.ArrayOrbitLightcurve

        q = (
            sa.select(m.ArrayOrbitLightcurve)
            .join(m.ArrayOrbitLightcurve.orbit)
            .order_by(
                m.Orbit.orbit_number.asc(), m.ArrayOrbitLightcurve.camera.asc()
            )
        )

        join_conditions = []
        filter_conditions = [LC.tic_id == tic_id]

        if aperture is None:
            join_conditions.append(BEST_LC.aperture_id == LC.aperture_id)
        else:
            q = q.join(LC.aperture)
            filter_conditions.append(m.Aperture.name == aperture)
        if lightcurve_type is None:
            join_conditions.append(
                BEST_LC.lightcurve_type_id == LC.lightcurve_type_id
            )
        else:
            q = q.join(LC.lightcurve_type)
            filter_conditions.append(m.LightcurveType.name == lightcurve_type)

        if len(join_conditions) > 0:
            join_conditions.append(BEST_LC.tic_id == LC.tic_id)
            join_conditions.append(BEST_LC.orbit_id == LC.orbit_id)
            q = q.join(BEST_LC, sa.and_(*join_conditions))
        q = q.where(*filter_conditions)

        # Ensure we grab lowest camera
        q = q.distinct(m.Orbit.orbit_number)

        return self._process_lc_selection(q)


class QLPMetricAPIMixin(APIMixin):
    """
    Provide interaction with the previously defined models here to avoid
    making the database connection object too large.
    """

    def get_qlp_stage(self, slug):
        stage = self.query(m.QLPStage).filter_by(slug=slug).one()
        return stage


class PGCatalogMixin(object):
    """
    Mixing to provide database objects PGCatalog API methods.
    """

    def get_pg_oid(self, tablename):
        """
        Obtain postgres's internal OID for the given tablename.

        Returns
        -------
        int or None:
            Returns the OID (int) of the given table or ``None`` if no such
            table exists.
        """
        return (
            self.query(psql_tables.PGClass.oid)
            .filter_by(relname=tablename)
            .one_or_none()
        )


class LegacyAPIMixin(APIMixin):
    """
    This mixin describes used methods convenient for QLP to implement
    minimally. These methods should be used as little as possible and instead
    either constructed manually or through more "generic" methods in other API
    mixins.
    """

    def get_all_orbit_ids(self):
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
        q = sa.select(m.Orbit.orbit_number).order_by(
            m.Orbit.orbit_number.asc()
        )
        self.execute(q).fetchall()

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
        q = sa.select(m.ArrayOrbitLightcurve)
        if apertures is not None:
            q = q.join(m.ArrayOrbitLightcurve.aperture)
            q = q.filter(m.Aperture.in_(apertures))
        if types is not None:
            q = q.join(m.ArrayOrbitLightcurve.lightcurve_type)
            q = q.filter(m.LightcurveType.name.in_(types))
        if tics is not None:
            q = q.filter(m.ArrayOrbitLightcurve.tic_id.in_(tics))
        return q

    def query_orbits_by_id(self, orbit_numbers):
        """Grab a numpy array representing the orbits"""
        orbits = (
            self.query(*m.Orbit.get_legacy_attrs())
            .filter(m.Orbit.orbit_number.in_(orbit_numbers))
            .order_by(m.Orbit.orbit_number)
        )
        return np.array(list(map(tuple, orbits)), dtype=m.Orbit.ORBIT_DTYPE)

    def query_frames_by_orbit(self, orbit_number, camera):
        """
        Determines the per-frame parameters of a given orbit, camera, and
        cadence type.

        Parameters
        ----------
        orbit_number : int
            The physical orbit number wanted.
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
        cols = [m.Orbit.orbit_number] + list(m.Frame.get_legacy_attrs())
        values = (
            self.query(*cols)
            .join(m.Frame.orbit)
            .join(m.Frame.frame_type)
            .filter(
                m.Frame.camera == camera,
                m.Orbit.orbit_number == orbit_number,
            )
            .distinct(m.Frame.cadence)
            .order_by(m.Frame.cadence.asc())
            .all()
        )

        return np.array(
            list(map(tuple, values)), dtype=m.Frame.FRAME_COMP_DTYPE
        )

    def query_frames_by_cadence(self, camera, cadences):
        columns = [m.Orbit.orbit_number] + list(m.Frame.get_legacy_attrs())

        q = (
            sa.select(*columns)
            .join(m.Frame.orbit)
            .filter(m.Frame.camera == camera, m.Frame.cadence.in_(cadences))
            .order_by(m.Frame.cadence.asc())
        )
        results = self.execute(q)
        return np.array(
            list(map(tuple, results)), dtype=m.Frame.FRAME_COMP_DTYPE
        )

    def get_cadences_in_orbits(self, orbit_numbers, frame_type=None):
        q = (
            sa.select(m.Frame.cadence.distinct())
            .join(m.Frame.orbit)
            .where(
                m.Orbit.orbit_number.in_(orbit_numbers),
            )
            .order_by(m.Frame.cadence.asc())
        )
        if frame_type is not None:
            q = q.join(m.Frame.frame_type).where(m.FrameType.name == frame_type)
        return self.execute(q).scalars().fetchall()

    def get_cadences_in_sectors(self, sectors, frame_type=None):
        q = (
            sa.select(m.Frame.cadence.distinct())
            .join(m.Frame.orbit)
            .where(
                m.Orbit.sector.in_(sectors),
            )
            .order_by(m.Frame.cadence.asc())
        )
        if frame_type is not None:
            q = q.join(m.Frame.frame_type).where(m.FrameType.name == frame_type)
        return self.execute(q).scalars().fetchall()
