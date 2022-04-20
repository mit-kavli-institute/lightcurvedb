import warnings

import numpy as np
from astropy import constants as const
from astropy import time
from loguru import logger
from scipy.interpolate import interp1d

from lightcurvedb.core.ingestors import contexts

LIGHTSPEED_AU_DAY = const.c.to("m/day") / const.au
BJD_EPOC = time.Time(2457000, format="jd", scale="tdb")


class LightcurveCorrector:
    def __init__(self, sqlite_path):
        bjd = contexts.get_spacecraft_data(sqlite_path, "bjd")
        tess_x = contexts.get_spacecraft_data(sqlite_path, "x")
        tess_y = contexts.get_spacecraft_data(sqlite_path, "y")
        tess_z = contexts.get_spacecraft_data(sqlite_path, "z")
        logger.debug("Loaded spacecraft time and position data")

        self.x_pos_interpolator = interp1d(bjd, tess_x)
        self.y_pos_interpolator = interp1d(bjd, tess_y)
        self.z_pos_interpolator = interp1d(bjd, tess_z)
        logger.debug("Built spacecraft position interpolations")

        self.tic_parameters = contexts.get_tic_mapping(
            sqlite_path, "ra", "dec", "tmag"
        )
        logger.debug("Built tic catalog mapping")
        self.quality_flag_map = contexts.get_quality_flag_mapping(sqlite_path)
        logger.debug("Built quality flag mapping")
        self.tjd_map = contexts.get_tjd_mapping(sqlite_path)
        logger.debug("Built tjd mapping")

    def correct_for_earth_time(self, tic_id, tjd_time_array):
        # Offset the bjd epoc for Earth time
        tjd_delta = time.TimeDelta(tjd_time_array, format="jd", scale="tdb")
        tjd_time = tjd_delta + BJD_EPOC

        # Interpolate the positions of the spacecraft with Earth time
        orbit_x = self.x_pos_interpolator(tjd_time.jd)
        orbit_y = self.y_pos_interpolator(tjd_time.jd)
        orbit_z = self.z_pos_interpolator(tjd_time.jd)
        orbit_vector = np.c_[orbit_x, orbit_y, orbit_z]

        parameters = self.tic_parameters[tic_id]
        ra = np.radians(parameters["ra"])
        dec = np.radians(parameters["dec"])
        star_vector = np.array(
            [np.cos(dec) * np.cos(ra), np.cos(dec) * np.sin(ra), np.sin(dec)]
        )

        try:
            light_time_to_earth = time.TimeDelta(
                np.dot(orbit_vector, star_vector) / LIGHTSPEED_AU_DAY,
                format="jd",
                scale="tdb",
            )
            bjd = tjd_time + light_time_to_earth - BJD_EPOC
        except (TypeError, ValueError):
            logger.exception(f"Bad Star Vector {star_vector} for TIC {tic_id}")
            raise
        return bjd.jd

    def get_magnitude_alignment_offset(
        self, tic_id, magnitudes, quality_flags
    ):
        tmag = self.tic_parameter[tic_id]["tmag"]
        mask = quality_flags == 0
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            return np.nanmedian(magnitudes[mask]) - tmag

    def get_quality_flags(self, camera, ccd, cadences):
        qflag_series = self.quality_flag_map[(camera, ccd)].loc[cadences]
        return qflag_series.to_numpy()

    def get_mid_tjd(self, camera, cadences):
        tjd_series = self.tjd_map[camera].loc[cadences]
        return tjd_series.to_numpy()
