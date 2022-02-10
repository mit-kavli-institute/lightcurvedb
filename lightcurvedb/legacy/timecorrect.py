from functools import lru_cache

import numpy as np
import pandas as pd
from astropy import constants as const
from astropy import time
from loguru import logger
from scipy.interpolate import interp1d

from lightcurvedb.core.ingestors.cache import IngestionCache
from lightcurvedb.core.ingestors.temp_table import TIC8Parameters
from lightcurvedb.models import Frame, SpacecraftEphemris

LIGHTSPEED_AU_DAY = const.c.to("m/day") / const.au
BJD_EPOC = time.Time(2457000, format="jd", scale="tdb")


@lru_cache(maxsize=1024)
def get_tic_parameters(tic_id, *parameters):
    with IngestionCache() as cache:
        cols = tuple(
            getattr(TIC8Parameters, parameter) for parameter in parameters
        )
        result = (
            cache.query(*cols)
            .filter(TIC8Parameters.tic_id == tic_id)
            .one_or_none()
        )
        if result is None:
            result = tuple(None for _ in parameters)
    return result


def timecorrect(ephemris_data, mid_tjd, ra, dec, bjd_offset=2457000):
    bjd_epoc = time.Time(bjd_offset, format="jd", scale="tdb")
    tjd_delta = time.TimeDelta(mid_tjd, format="jd", scale="tdb")
    tjd_time = tjd_delta + bjd_epoc

    tess_x_interpolator = interp1d(
        ephemris_data.barycentric_dynamical_time, ephemris_data.x_coordinate
    )
    tess_y_interpolator = interp1d(
        ephemris_data.barycentric_dynamical_time, ephemris_data.y_coordinate
    )
    tess_z_interpolator = interp1d(
        ephemris_data.barycentric_dynamical_time, ephemris_data.z_coordinate
    )

    orbit_x = tess_x_interpolator(tjd_time.jd)
    orbit_y = tess_y_interpolator(tjd_time.jd)
    orbit_z = tess_z_interpolator(tjd_time.jd)
    orbit_vector = np.c_[orbit_x, orbit_y, orbit_z]

    # Radian conversion
    ra = ra / 180.0 * np.pi
    dec = dec / 180.0 * np.pi
    star_vector = np.array(
        [np.cos(dec) * np.cos(ra), np.cos(dec) * np.sin(ra), np.sin(dec)]
    )

    # Calculate time arrival to Earth
    light_time = time.TimeDelta(
        np.dot(orbit_vector, star_vector) / LIGHTSPEED_AU_DAY,
        format="jd",
        scale="tdb",
    )
    bjd = tjd_time + light_time - bjd_epoc

    return bjd.jd


class TimeCorrector:
    def __init__(self, session, cache):
        q = session.query(
            SpacecraftEphemris.barycentric_dynamical_time,
            SpacecraftEphemris.x,
            SpacecraftEphemris.y,
            SpacecraftEphemris.z,
        )
        self.ephemris = pd.read_sql(q.statement, session.bind)

        self.tess_x_interpolator = interp1d(
            self.ephemris.barycentric_dynamical_time,
            self.ephemris.x_coordinate,
        )
        self.tess_y_interpolator = interp1d(
            self.ephemris.barycentric_dynamical_time,
            self.ephemris.y_coordinate,
        )
        self.tess_z_interpolator = interp1d(
            self.ephemris.barycentric_dynamical_time,
            self.ephemris.z_coordinate,
        )

    def get_tic_params(self, tic):
        ra, dec, tmag = get_tic_parameters(
            tic, "right_ascension", "declination", "tmag"
        )
        return {
            "ra": ra,
            "dec": dec,
            "tmag": tmag,
        }

    def correct(self, tic, mid_tjd_array):
        tjd_delta = time.TimeDelta(mid_tjd_array, format="jd", scale="tdb")
        tjd_time = tjd_delta + BJD_EPOC

        orbit_x = self.tess_x_interpolator(tjd_time.jd)
        orbit_y = self.tess_y_interpolator(tjd_time.jd)
        orbit_z = self.tess_z_interpolator(tjd_time.jd)
        orbit_vector = np.c_[orbit_x, orbit_y, orbit_z]

        # Radian conversion
        row = self.get_tic_params(tic)
        ra = np.radians(row["ra"])
        dec = np.radians(row["dec"])
        star_vector = np.array(
            [np.cos(dec) * np.cos(ra), np.cos(dec) * np.sin(ra), np.sin(dec)]
        )

        try:
            # Calculate light time arrival to Earth
            light_time = time.TimeDelta(
                np.dot(orbit_vector, star_vector) / LIGHTSPEED_AU_DAY,
                format="jd",
                scale="tdb",
            )
            bjd = tjd_time + light_time - BJD_EPOC
        except (TypeError, ValueError):
            logger.error(
                "Bad Star Vector: {0} for TIC ID {1}\n".format(
                    star_vector, tic
                )
            )
            raise
        return bjd.jd


class StaticTimeCorrector(TimeCorrector):
    def __init__(self, session):
        q = session.query(
            SpacecraftEphemris.barycentric_dynamical_time,
            SpacecraftEphemris.x,
            SpacecraftEphemris.y,
            SpacecraftEphemris.z,
        )
        self.ephemris = pd.read_sql(q.statement, session.bind)
        q = (
            session.query(
                Frame.cadence,
                Frame.camera,
                Frame.mid_tjd,
            )
            .order_by(Frame.cadence, Frame.camera)
            .filter(Frame.frame_type_id == "Raw FFI")
        )
        self.mid_tjd_map = pd.read_sql(
            q.statement, session.bind, index_col=["cadence", "camera"]
        ).sort_index()

        self.mid_tjd_map = self.mid_tjd_map[
            ~self.mid_tjd_map.index.duplicated(keep="last")
        ]

        self.tess_x_interpolator = interp1d(
            self.ephemris.barycentric_dynamical_time,
            self.ephemris.x_coordinate,
        )
        self.tess_y_interpolator = interp1d(
            self.ephemris.barycentric_dynamical_time,
            self.ephemris.y_coordinate,
        )
        self.tess_z_interpolator = interp1d(
            self.ephemris.barycentric_dynamical_time,
            self.ephemris.z_coordinate,
        )

    def correct_bjd(self, ra, dec, lightpoints):
        mid_tjd_array = self.mid_tjd(lightpoints)
        tjd_delta = time.TimeDelta(mid_tjd_array, format="jd", scale="tdb")
        tjd_time = tjd_delta + BJD_EPOC

        orbit_x = self.tess_x_interpolator(tjd_time.jd)
        orbit_y = self.tess_y_interpolator(tjd_time.jd)
        orbit_z = self.tess_z_interpolator(tjd_time.jd)
        orbit_vector = np.c_[orbit_x, orbit_y, orbit_z]

        ra = np.radians(ra)
        dec = np.radians(dec)

        star_vector = np.array(
            [np.cos(dec) * np.cos(ra), np.cos(dec) * np.sin(ra), np.sin(dec)]
        )

        try:
            # Calculate light time arrival to Earth
            light_time = time.TimeDelta(
                np.dot(orbit_vector, star_vector) / LIGHTSPEED_AU_DAY,
                format="jd",
                scale="tdb",
            )
            bjd = tjd_time + light_time - BJD_EPOC
        except ValueError:
            logger.error(
                "Star Vector: {0}\n"
                "Orbit Vector: {1}\n"
                "tjd: {2}\n".format(star_vector, orbit_vector, tjd_time)
            )
            raise
        return bjd.jd

    def mid_tjd(self, lightpoint_df):
        df = lightpoint_df.reset_index()
        index = [tuple(r) for r in df[["cadences", "camera"]].values]
        return self.mid_tjd_map.loc[index]["mid_tjd"].values


class PartitionTimeCorrector(StaticTimeCorrector):
    def mid_tjd(self, lightpoint_df):
        index = [tuple(r) for r in lightpoint_df[["cadence", "camera"]].values]
        return self.mid_tjd_map.loc[index]["mid_tjd"].values
