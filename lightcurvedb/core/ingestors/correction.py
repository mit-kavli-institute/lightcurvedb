import warnings
from datetime import datetime

import numpy as np
import pyticdb
from astropy import constants as const
from astropy import time
from loguru import logger
from scipy.interpolate import interp1d

from lightcurvedb.core.ingestors import contexts

LIGHTSPEED_AU_DAY = const.c.to("m/day") / const.au
BJD_EPOC = time.Time(2457000, format="jd", scale="tdb")

TIC_PARAM_FIELDS = (
    "ra",
    "dec",
    "tmag",
    "pmra",
    "pmdec",
    "jmag",
    "kmag",
    "vmag",
)


class LightcurveCorrector:
    def __init__(self, sqlite_path):
        self.sqlite_path = sqlite_path
        bjd = contexts.get_spacecraft_data(sqlite_path, "bjd")
        tess_x = contexts.get_spacecraft_data(sqlite_path, "x")
        tess_y = contexts.get_spacecraft_data(sqlite_path, "y")
        tess_z = contexts.get_spacecraft_data(sqlite_path, "z")
        logger.debug("Loaded spacecraft time and position data")

        self.x_pos_interpolator = interp1d(bjd, tess_x)
        self.y_pos_interpolator = interp1d(bjd, tess_y)
        self.z_pos_interpolator = interp1d(bjd, tess_z)
        logger.debug("Built spacecraft position interpolations")
        self.quality_flag_map = contexts.get_quality_flag_mapping(sqlite_path)
        logger.debug("Built quality flag mapping")
        self.tjd_map = contexts.get_tjd_mapping(sqlite_path)
        logger.debug("Built tjd mapping")
        self.tic_map = contexts.get_tic_mapping(
            sqlite_path, "ra", "dec", "tmag"
        )
        logger.debug("Got ra-dec mapping")

        self._last_tic_miss = None

    def resolve_tic_parameters(self, tic_id, *fields):
        try:
            row = self.tic_map[tic_id]
        except KeyError:
            remote = pyticdb.query_by_id(tic_id, *TIC_PARAM_FIELDS)
            row = dict(zip(TIC_PARAM_FIELDS, remote))
            self.tic_map[tic_id] = row

            if self._last_tic_miss is None:
                self._last_tic_miss = datetime.now()
            else:
                elapsed = datetime.now() - self._last_tic_miss
                self._last_tic_miss = datetime.now()
                if elapsed.seconds < 5:
                    logger.warning(
                        "High subsequent hits to TIC. "
                        "Is the catalog representative of "
                        "the ingesting orbit?"
                    )

        result = tuple(row[field] for field in fields)
        return result

    def correct_for_earth_time(self, tic_id, tjd_time_array):
        # Offset the bjd epoc for Earth time
        tjd_delta = time.TimeDelta(tjd_time_array, format="jd", scale="tdb")
        tjd_time = tjd_delta + BJD_EPOC

        # Interpolate the positions of the spacecraft with Earth time
        orbit_x = self.x_pos_interpolator(tjd_time.jd)
        orbit_y = self.y_pos_interpolator(tjd_time.jd)
        orbit_z = self.z_pos_interpolator(tjd_time.jd)
        orbit_vector = np.c_[orbit_x, orbit_y, orbit_z]

        ra, dec = self.resolve_tic_parameters(tic_id, "ra", "dec")
        ra = np.radians(ra)
        dec = np.radians(dec)
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
            bjd = tjd_time
        return bjd.jd

    def get_magnitude_alignment_offset(
        self, tic_id, magnitudes, quality_flags
    ):
        tmag = self.resolve_tic_parameters(tic_id, "tmag")
        mask = quality_flags == 0
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            return np.nanmedian(magnitudes[mask]) - tmag

    def get_quality_flags(self, camera, ccd, cadences):
        qflag_series = self.quality_flag_map.loc[(camera, ccd)].loc[cadences][
            "quality_flag"
        ]
        return qflag_series.to_numpy()

    def get_mid_tjd(self, camera, cadences):
        camera_tjds = self.tjd_map.loc[camera]
        tjd = camera_tjds.loc[cadences]["tjd"]
        return tjd.to_numpy()
