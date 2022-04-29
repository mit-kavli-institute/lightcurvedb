"""
Generate data and push to a static file to test for ingestion correctness
"""
import pathlib
from collections import namedtuple

import numpy as np
from astropy.io import fits
from astropy.time import Time
from h5py import File as H5File
from hypothesis import note
from hypothesis import strategies as st
from hypothesis.extra import numpy as np_st
from hypothesis.extra import pandas as pd_st

from lightcurvedb.core.ingestors import contexts
from lightcurvedb.models.lightpoint import get_lightpoint_dtypes

from . import orm as orm_st
from . import tess as tess_st

camera_quaternion = namedtuple(
    "camera_quaternion",
    [
        "gps_time",
        "q1",
        "q2",
        "q3",
        "q4",
        "bit_check",
        "total_guide_stars",
        "valid_guide_stars",
    ],
)


CATALOG_KEY_ORDER = (
    "tic_id",
    "ra",
    "dec",
    "tmag",
    "pmra",
    "pmdec",
    "jmag",
    "kmag",
    "vmag",
)


@st.composite
def cadences(draw, **overrides):
    length = draw(overrides.get("length", st.just(20)))
    cadences = np.arange(0, length, step=1, dtype=int)
    return draw(st.just(cadences))


@st.composite
def julian_dates(draw, **overrides):
    length = draw(overrides.get("length", st.just(20)))
    exposure_time = draw(overrides.get("exposure_time", st.just(200)))
    exposure_step_days = exposure_time / 60 / 60 / 24

    julian_days = np.arange(0, length) * exposure_step_days
    return draw(st.just(julian_days))


@st.composite
def camera_quaternions(draw):
    return draw(
        st.builds(
            camera_quaternion,
            gps_time=tess_st.gps_times(),
            q1=st.floats(allow_nan=False, allow_infinity=False),
            q2=st.floats(allow_nan=False, allow_infinity=False),
            q3=st.floats(allow_nan=False, allow_infinity=False),
            q4=st.floats(allow_nan=False, allow_infinity=False),
            bit_check=st.integers(),
            total_guide_stars=st.integers(),
            valid_guide_stars=st.integers(),
        )
    )


@st.composite
def ffi_headers(draw):
    return draw(
        st.builds(
            dict,
            SIMPLE=st.just(True),
            BITPIX=st.just(32),
            NAXIS=st.just(2),
            NAXIS1=st.integers(min_value=1, max_value=2048),
            NAXIS2=st.integers(min_value=1, max_value=2048),
            EXTEND=st.just(True),
            BSCALE=st.just(1),
            BZERO=st.just(2147483648),
            CRM_N=st.just(10),
            ORBIT_ID=tess_st.orbits(),
            ACS_MODE=st.one_of(st.just("FP"), st.just("CPI")),
            SC_RA=tess_st.right_ascensions(),
            SC_DEC=tess_st.declinations(),
            SC_ROLL=tess_st.rolls(),
            SC_QUATX=st.floats(min_value=0, max_value=1),
            SC_QUATY=st.floats(min_value=0, max_value=1),
            SC_QUATZ=st.floats(min_value=0, max_value=1),
            SC_QUATQ=st.floats(min_value=0, max_value=1),
            TJD_ZERO=st.just(2457000.0),
            STARTTJD=st.floats(allow_nan=False, allow_infinity=False),
            MIDTJD=st.floats(allow_nan=False, allow_infinity=False),
            ENDTJD=st.floats(allow_nan=False, allow_infinity=False),
            EXPTIME=st.floats(allow_nan=False, allow_infinity=False),
            INT_TIME=orm_st.psql_small_integers(),
            TIME=tess_st.gps_times(),
            PIX_CAT=st.just(0),
            REQUANT=st.just(405),
            DIFF_HUF=st.just(109),
            QUAL_BIT=st.just(0),
            SPM=st.just(2),
            CAM=tess_st.cameras(),
            CADENCE=tess_st.cadences(),
            CRM=st.just(True),
        )
    )


@st.composite
def apertures(draw):
    return draw(st.text(min_size=1, max_size=64))


@st.composite
def lightcurve_types(draw):
    return draw(st.text(min_size=1, max_size=64))


@st.composite
def lightpoints(draw, **overrides):
    return draw(
        st.builds(
            dict,
            cadence=tess_st.cadences(),
            barycentric_julian_date=tess_st.tjds(),
            data=st.floats(),
            error=st.floats(),
            x_centroid=st.floats(allow_nan=False, allow_infinity=False),
            y_centroid=st.floats(allow_nan=False, allow_infinity=False),
            quality_flag=st.integers(min_value=0, max_value=1),
        )
    )


@st.composite
def lightpoint_dfs(draw):
    return draw(
        pd_st.data_frames(
            [
                pd_st.column("cadence", dtype=int),
                pd_st.column("barycentric_julian_date", dtype=float),
                pd_st.column("data", dtype=float),
                pd_st.column("error", dtype=float),
                pd_st.column("x_centroid", dtype=float),
                pd_st.column("y_centroid", dtype=float),
                pd_st.column("quality_flag", dtype=float),
            ]
        )
    )


@st.composite
def shallow_lightpoints(draw):
    return draw(
        st.builds(
            dict,
            data=st.floats(),
            error=st.floats(),
            x_centroid=st.floats(allow_nan=False, allow_infinity=False),
            y_centroid=st.floats(allow_nan=False, allow_infinity=False),
            quality_flag=st.integers(min_value=0, max_value=1),
        )
    )


@st.composite
def shallow_lightpoint_dfs(draw):
    return draw(
        pd_st.data_frames(
            [
                pd_st.column("data", dtype=float),
                pd_st.column("error", dtype=float),
                pd_st.column("x_centroid", dtype=float),
                pd_st.column("y_centroid", dtype=float),
                pd_st.column("quality_flag", dtype=float),
            ]
        )
    )


@st.composite
def lightpoint_arrays(draw, **overrides):
    length = draw(overrides.get("length", st.just(20)))
    cadence_sample = draw(cadences(length=st.just(length)))
    bjd = draw(julian_dates(length=st.just(length)))
    data = np.random.rand(length)
    error = np.random.rand(length)
    x_centroids = np.random.rand(length)
    y_centroids = np.random.rand(length)
    quality_flags = np.random.randint(2, size=length)

    array = np.empty(
        length,
        dtype=get_lightpoint_dtypes(
            "cadence",
            "barycentric_julian_date",
            "data",
            "error",
            "x_centroid",
            "y_centroid",
            "quality_flag",
        ),
    )
    array["cadence"] = cadence_sample
    array["barycentric_julian_date"] = bjd
    array["data"] = data
    array["error"] = error
    array["x_centroid"] = x_centroids
    array["y_centroid"] = y_centroids
    array["quality_flag"] = quality_flags
    return draw(st.just(array))


@st.composite
def shallow_lightpoint_arrays(draw, **overrides):
    length = draw(overrides.get("length", st.just(20)))
    data = np.random.rand(length)
    error = np.random.rand(length)
    x_centroids = np.random.rand(length)
    y_centroids = np.random.rand(length)
    quality_flags = np.random.randint(2, size=length)

    array = np.empty(
        length,
        dtype=get_lightpoint_dtypes(
            "data",
            "error",
            "x_centroid",
            "y_centroid",
            "quality_flag",
        ),
    )
    array["data"] = data
    array["error"] = error
    array["x_centroid"] = x_centroids
    array["y_centroid"] = y_centroids
    array["quality_flag"] = quality_flags
    return draw(st.just(array))


@st.composite
def lightcurves(draw, **overrides):
    return draw(
        st.builds(
            dict,
            tic_id=overrides.get("tic_id", tess_st.tic_ids()),
            aperture_id=apertures(),
            lightcurve_type_id=lightcurve_types(),
        )
    )


# Begin Simulation Functions
def simulate_hk_file(data, directory, formatter=str, **overrides):
    quaternions = data.draw(
        st.lists(camera_quaternions(), unique_by=lambda cq: cq[0])
    )
    camera = data.draw(overrides.get("camera", tess_st.cameras()))
    filename = pathlib.Path(f"cam{camera}_quat.txt")
    path = pathlib.Path(directory) / filename

    with open(path, "wt") as fout:
        for row in quaternions:
            line = " ".join(map(formatter, row))
            fout.write(line)
            fout.write("\n")
    return path, camera, quaternions


def simulate_fits(data, directory):
    header = data.draw(ffi_headers())
    ffi_data = data.draw(
        np_st.arrays(np.int32, (header["NAXIS1"], header["NAXIS2"]))
    )
    cam = header["CAM"]
    cadence = header["CADENCE"]

    start = Time(header["TIME"], format="gps")
    basename_time = str(start.iso).replace("-", "")

    filename = pathlib.Path(
        f"tess{basename_time}-{cadence:08}-{cam}-crm-ffi.fits"
    )

    hdu = fits.PrimaryHDU(ffi_data)
    hdr = hdu.header
    hdul = fits.HDUList([hdu])

    for key, value in header.items():
        hdr[key] = value
    hdul.writeto(directory / filename, overwrite=True)
    return directory / filename, header


def simulate_h5_file(
    data,
    directory,
    background_aperture,
    background_type,
    apertures,
    lightcurve_types,
    **overrides,
):
    tic_id = data.draw(overrides.get("tic_id", tess_st.tic_ids()))
    filename = pathlib.Path(f"{tic_id}.h5")

    background_lc = data.draw(lightpoint_arrays())
    data_gen_ref = {
        "tic_id": tic_id,
        "background": background_lc,
        "photometry": {},
    }

    with H5File(directory / filename, "w") as h5:
        lc = h5.create_group("LightCurve")
        lc.create_dataset("Cadence", data=background_lc["cadence"][0])
        lc.create_dataset(
            "BJD", data=background_lc["barycentric_julian_date"][0]
        )
        lc.create_dataset("X", data=background_lc["x_centroid"][0])
        lc.create_dataset("Y", data=background_lc["y_centroid"][0])

        photometry = lc.create_group("AperturePhotometry")
        for ap in apertures:
            note(photometry.keys())
            try:
                api = photometry.create_group(ap)
            except ValueError:
                raise ValueError(f"{photometry.keys()}")
            api.create_dataset("X", data=background_lc["x_centroid"][0])
            api.create_dataset("Y", data=background_lc["y_centroid"][0])
            api.create_dataset(
                "QualityFlag", data=background_lc["quality_flag"][0]
            )

            for lightcurve_type in lightcurve_types:
                sample = data.draw(shallow_lightpoint_arrays())
                try:
                    data_gen_ref["photometry"][(ap, lightcurve_type)] = sample
                    api.create_dataset(lightcurve_type, data=sample["data"][0])
                    api.create_dataset(
                        f"{lightcurve_type}Error", data=sample["error"][0]
                    )
                except ValueError:
                    raise ValueError(f"{api.keys()}")

    return directory / filename, data_gen_ref


def simulate_tic_catalog(data, directory):
    filename = data.draw(
        st.from_regex(r"^catalog_[0-9]+_[1-4]_[1-4]_(bright|full)\.txt$")
    )

    tic_parameters = data.draw(
        st.lists(
            tess_st.tic_parameters(), unique_by=lambda param: param["tic_id"]
        )
    )

    with open(directory / pathlib.Path(filename), "wt") as fout:
        for param in tic_parameters:
            msg = " ".join(map(str, (param[key] for key in CATALOG_KEY_ORDER)))
            fout.write(msg)
            fout.write("\n")
    return directory / pathlib.Path(filename), tic_parameters


def simulate_quality_flag_file(data, directory, **overrides):
    camera = data.draw(overrides.get("camera", tess_st.cameras()))
    ccd = data.draw(overrides.get("ccd", tess_st.ccds()))

    quality_flags = data.draw(
        overrides.get(
            "cadences",
            st.lists(
                st.tuples(
                    tess_st.cadences(), st.integers(min_value=0, max_value=1)
                ),
                unique_by=lambda qflag: qflag[0],
            ),
        )
    )

    filename = pathlib.Path(f"cam{camera}ccd{ccd}_qflag.txt")

    with open(directory / filename, "wt") as fout:
        for cadence, flag in quality_flags:
            fout.write(f"{cadence} {flag}\n")

    return directory / filename, camera, ccd, quality_flags


@contexts.with_sqlite
def _add_quality_flag_to_cache(conn, camera, ccd, cadence, flag):
    with conn:
        conn.execute(
            "INSERT INTO quality_flags(camera, ccd, cadence, quality_flag) "
            "VALUES (?, ?, ?, ?)",
            (camera, ccd, cadence, flag),
        )


@contexts.with_sqlite
def _add_tic_parameters(conn, tic_parameters):
    with conn:
        conn.executemany(
            "INSERT INTO tic_parameters("
            "tic_id, ra, dec, tmag, pmra, pmdec, jmag, kmag, vmag"
            ") VALUES ("
            ":tic_id, :ra, :dec, :tmag, :pmra, :pmdec, :jmag, :kmag, :vmag"
            ")",
            tic_parameters,
        )


# manual cleanup!
def simulate_lightcurve_ingestion_environment(
    data,
    directory,
    db,
    lightcurve_length=20,
):
    # Put critical assumptions here
    cadences = np.arange(0, lightcurve_length)
    exposure_time = data.draw(
        st.floats(
            min_value=2,
            max_value=30 * 60,
            allow_infinity=False,
            allow_nan=False,
        )
    )

    cache_name = pathlib.Path("db.sqlite3")
    cache_path = directory / cache_name
    camera = data.draw(tess_st.cameras())
    ccd = data.draw(tess_st.ccds())

    contexts.make_shared_context(cache_path)

    cur_start_tjd = 0.0
    cur_end_tjd = cur_start_tjd + (exposure_time / 60 / 60 / 24)

    frame_type = data.draw(orm_st.frame_types())
    orbit = data.draw(orm_st.orbits())
    orbit.id = 1
    db.add(orbit)
    db.add(frame_type)

    background_type, *magnitude_types = data.draw(
        st.lists(
            orm_st.lightcurve_types(),
            min_size=2,
            max_size=3,
            unique_by=lambda lt: lt.name,
        )
    )

    background_aperture, *photometric_apertures = data.draw(
        st.lists(
            orm_st.apertures(),
            min_size=2,
            max_size=5,
            unique_by=(
                lambda ap: ap.name,
                lambda ap: (ap.star_radius, ap.outer_radius, ap.inner_radius),
            ),
        )
    )
    db.add(background_type)
    db.add(background_aperture)
    db.session.add_all(magnitude_types)
    db.session.add_all(photometric_apertures)
    db.flush()

    for cadence in cadences:
        cur_mid_tjd = cur_start_tjd + (exposure_time / 60 / 60 / 24 / 2)
        frame = data.draw(
            orm_st.frames(
                cadence=st.just(cadence),
                camera=st.just(camera),
                exp_time=st.just(exposure_time),
                start_tjd=st.just(cur_start_tjd),
                mid_tjd=st.just(cur_mid_tjd),
                end_tjd=st.just(cur_end_tjd),
                orbit=st.just(orbit),
                frame_type=st.just(frame_type),
            )
        )
        frame.file_path = f"{frame.cadence}_{frame.file_path}"

        cur_start_tjd = cur_end_tjd
        cur_end_tjd = cur_start_tjd + (exposure_time / 60 / 60 / 24)

        db.add(frame)
        db.flush()
        _add_quality_flag_to_cache(
            cache_path, camera, ccd, cadence, int(frame.quality_bit)
        )

    eph = data.draw(
        st.lists(
            orm_st.spacecraft_ephemris(),
            unique_by=lambda eph: eph.barycentric_dynamical_time,
        )
    )
    db.session.add_all(eph)
    db.flush()

    contexts.populate_ephemris(cache_path, db)
    contexts.populate_tjd_mapping(cache_path, db)

    # Generate tic parameters
    tic_parameters = data.draw(
        st.lists(tess_st.tic_parameters(), min_size=1, max_size=1)
    )
    _add_tic_parameters(cache_path, tic_parameters)

    for parameters in tic_parameters:
        simulate_h5_file(
            data,
            directory,
            background_aperture.name,
            background_type.name,
            [ap.name for ap in photometric_apertures],
            [type.name for type in magnitude_types],
            tic_id=st.just(parameters["tic_id"]),
        )
