"""
Generate data and push to a static file to test for ingestion correctness
"""
import pathlib
from collections import namedtuple
from itertools import product
from shutil import rmtree

import numpy as np
from astropy.io import fits
from astropy.time import Time
from h5py import File as H5File
from hypothesis import note
from hypothesis import strategies as st
from hypothesis.extra import numpy as np_st
from hypothesis.extra import pandas as pd_st

from lightcurvedb.models.camera_quaternion import get_utc_time

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
            x_centroid=st.floats(
                allow_nan=False, allow_infinity=False, width=32
            ),
            y_centroid=st.floats(
                allow_nan=False, allow_infinity=False, width=32
            ),
            quality_flag=st.integers(min_value=0, max_value=1),
        )
    )


def lightpoint_dfs():
    return pd_st.data_frames(
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


def lightpoint_dtypes():
    return st.just(
        np.dtype(
            [
                ("cadence", int),
                ("barycentric_julian_date", float),
                ("data", float),
                ("error", float),
                ("x_centroid", float),
                ("y_centroid", float),
                ("quality_flag", float),
            ]
        )
    )


def lightpoint_arrays():
    return np_st.arrays(
        dtype=lightpoint_dtypes(),
        shape=st.tuples(st.integers(min_value=1, max_value=1000), st.just(6)),
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
def lightcurves(draw, **overrides):
    return draw(
        st.builds(
            dict,
            tic_id=overrides.get("tic_id", tess_st.tic_ids()),
            aperture_id=apertures(),
            lightcurve_type_id=lightcurve_types(),
        )
    )


def _unique_gps(camera_quat_model):
    gps_time = camera_quat_model.gps_time
    return get_utc_time(gps_time).datetime.replace(microsecond=0)


# Begin Simulation Functions
def simulate_hk_file(data, directory, formatter=str, **overrides):
    quaternions = data.draw(
        st.lists(camera_quaternions(), min_size=1, unique_by=_unique_gps)
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
        f"tess{basename_time}-{cadence: 08}-{cam}-crm-ffi.fits"
    )

    hdu = fits.PrimaryHDU(ffi_data)
    hdr = hdu.header
    hdul = fits.HDUList([hdu])

    for key, value in header.items():
        hdr[key] = value
    hdul.writeto(directory / filename, overwrite=True)
    return directory / filename, header


def simulate_tica_header(
    exposure_time: float, camera: int, ccd: int, cadence: int
):
    return st.fixed_dictionaries(
        {
            "SIMPLE": st.just(True),
            "BITPIX": st.just(32),
            "NAXIS": st.just(2),
            "NAXIS1": st.integers(min_value=1, max_value=2048),
            "NAXIS2": st.integers(min_value=1, max_value=2048),
            "EXTEND": st.just(True),
            "BSCALE": st.just(1),
            "BZERO": st.just(2147483648),
            "ORBIT_ID": tess_st.orbits(),
            "ACS_MODE": st.one_of(st.just("FP"), st.just("CPI")),
            "SC_RA": tess_st.right_ascensions(),
            "SC_DEC": tess_st.declinations(),
            "SC_ROLL": tess_st.rolls(),
            "SC_QUATX": st.floats(min_value=0, max_value=1),
            "SC_QUATY": st.floats(min_value=0, max_value=1),
            "SC_QUATZ": st.floats(min_value=0, max_value=1),
            "SC_QUATQ": st.floats(min_value=0, max_value=1),
            "TJD_ZERO": st.just(2457000.0),
            "INT_TIME": st.just(int(exposure_time * cadence)),
            "CAMNUM": st.just(camera),
            "CADENCE": st.just(cadence),
            "TIME": st.just(exposure_time * cadence),
            "STARTTJD": st.just(exposure_time * cadence),
            "MIDTJD": st.just(exposure_time * cadence),
            "ENDTJD": st.just(exposure_time * cadence),
            "EXPTIME": st.just(exposure_time),
            "QUAL_BIT": st.booleans(),
            "FINE": st.booleans(),
            "COARSE": st.booleans(),
            "RW_DESAT": st.booleans(),
            "PIX_CAT": st.just(0),
            "REQUANT": st.just(411),
            "DIFF_HUF": st.just(10407),
            "PRIM_HUF": st.just(17),
            "SPM": st.just(2),
            "CRM": st.just(True),
            "ORB_SEG": st.sampled_from(["o2a", "o1a", "o1b", "o2b"]),
            "SCIPIXS": st.just("[45:2092,1:2048]"),
            "GAIN_A": st.floats(allow_infinity=False, allow_nan=False),
            "GAIN_B": st.floats(allow_infinity=False, allow_nan=False),
            "GAIN_C": st.floats(allow_infinity=False, allow_nan=False),
            "GAIN_D": st.floats(allow_infinity=False, allow_nan=False),
            "UNITS": st.just("electrons"),
            "EQUINOX": st.just(2000),
            "INSTRUME": st.just("TESS Photometer"),
            "MJD-BEG": st.floats(allow_infinity=False, allow_nan=False),
            "MJD-END": st.floats(allow_infinity=False, allow_nan=False),
            "TESS_X": st.floats(allow_infinity=False, allow_nan=False),
            "TESS_Y": st.floats(allow_infinity=False, allow_nan=False),
            "TESS_Z": st.floats(allow_infinity=False, allow_nan=False),
            "TESS_VX": st.floats(allow_infinity=False, allow_nan=False),
            "TESS_VY": st.floats(allow_infinity=False, allow_nan=False),
            "TESS_VZ": st.floats(allow_infinity=False, allow_nan=False),
            "RA_TARG": tess_st.right_ascensions(),
            "DEC_TARG": tess_st.declinations(),
            "WCSGDF": st.floats(allow_infinity=False, allow_nan=False),
            "CHECKSUM": st.integers(),
            "DATASUM": st.integers(),
            "COMMENT": st.just("comment"),
        },
        optional={"CCDNUM": st.just(ccd), "CCD": st.just(ccd)},
    ).filter(lambda header: ("CCD" in header) ^ ("CCDNUM" in header))


def simulate_tica_fits(
    data, raw_directory: pathlib.Path
) -> tuple[list[pathlib.Path], list[dict]]:
    sector = data.draw(tess_st.sectors())
    cadences = data.draw(st.lists(st.integers(min_value=1), unique=True))
    cameras = [1, 2, 3, 4]
    ccds = [1, 2, 3, 4]
    exposure_time = data.draw(
        st.sampled_from([158.4, 1437.76728, 479.25576, 159.75192])
    )
    sector_string = f"s{sector: 04d}"
    sector_directory = raw_directory / sector_string

    if sector_directory.exists():
        rmtree(sector_directory)
    sector_directory.mkdir(parents=True)

    for camera, ccd in product(cameras, ccds):
        subdir = sector_directory / f"cam{camera}-ccd{ccd}"
        subdir.mkdir()

    files: list[pathlib.Path] = []
    headers: list[dict] = []

    for camera, ccd, cadence in product(cameras, ccds, cadences):
        subdir = sector_directory / f"cam{camera}-ccd{ccd}"

        header = data.draw(
            simulate_tica_header(exposure_time, camera, ccd, cadence)
        )
        ffi_data = data.draw(
            np_st.arrays(np.int32, (header["NAXIS1"], header["NAXIS2"]))
        )
        hdu = fits.PrimaryHDU(ffi_data)
        hdr = hdu.header
        hdul = fits.HDUList([hdu])

        for key, value in header.items():
            hdr[key] = value

        ccd = header.get("CCD", header.get("CCDNUM", None))

        # hlsp_tica_tess_ffi_s0081-o2-00990440-cam1-ccd1_tess_v01_img.fits
        filename = (
            f"hlsp_tica_tess_ffi_{sector_string}-{header['ORB_SEG'][:2]}"
            f"-{cadence: 08d}-cam{header['CAMNUM']}-ccd{ccd}_tess"
            "v01_img.fits"
        )
        hdul.writeto(subdir / filename, overwrite=True)
        files.append(subdir / filename)
        headers.append(header)

    return files, headers


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
        lc.create_dataset("Cadence", data=background_lc["cadence"])
        lc.create_dataset("BJD", data=background_lc["barycentric_julian_date"])
        lc.create_dataset("X", data=background_lc["x_centroid"])
        lc.create_dataset("Y", data=background_lc["y_centroid"])

        photometry = lc.create_group("AperturePhotometry")
        for ap in apertures:
            note(str(photometry.keys()))
            api = photometry.create_group(ap)
            api.create_dataset("X", data=background_lc["x_centroid"])
            api.create_dataset("Y", data=background_lc["y_centroid"])
            api.create_dataset(
                "QualityFlag", data=background_lc["quality_flag"]
            )

            for lightcurve_type in lightcurve_types:
                sample = data.draw(lightpoint_arrays())
                data_gen_ref["photometry"][(ap, lightcurve_type)] = sample
                try:
                    api.create_dataset(lightcurve_type, data=sample["data"])
                except ValueError:
                    raise ValueError(
                        f"{api.keys()}, {lightcurve_type}, {lightcurve_types}"
                    )
                api.create_dataset(
                    f"{lightcurve_type}Error", data=sample["error"]
                )
        bg = lc.create_group(background_type)
        bg.create_dataset("Value", data=background_lc["data"])
        bg.create_dataset("Error", data=background_lc["error"])

    return directory / filename, data_gen_ref


def _write_tic_catalog(path, parameters):
    with open(path, "wt") as fout:
        for param in parameters:
            msg = " ".join(map(str, (param[key] for key in CATALOG_KEY_ORDER)))
            fout.write(msg)
            fout.write("\n")
    return path


def simulate_tic_catalog(data, directory):
    filename = data.draw(
        st.from_regex(r"^catalog_[0-9]+_[1-4]_[1-4]_(bright|full)\.txt$")
    )

    tic_parameters = data.draw(
        st.lists(
            tess_st.tic_parameters(), unique_by=lambda param: param["tic_id"]
        )
    )
    catalog_path = _write_tic_catalog(
        directory / pathlib.Path(filename), tic_parameters
    )
    return catalog_path, tic_parameters


def _write_quality_flag_file(path, quality_flags):
    with open(path, "wt") as fout:
        for cadence, flag in quality_flags:
            fout.write(f"{cadence} {flag}\n")

    return path


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

    quality_flag_path = _write_quality_flag_file(
        directory / filename, quality_flags
    )

    return quality_flag_path, camera, ccd, quality_flags


# requires manual cleanup!
def simulate_lightcurve_ingestion_environment(
    data,
    directory,
    db,
    lightcurve_length=20,
):
    # Put critical assumptions here
    cadences = np.arange(0, lightcurve_length)
    exposure_time = 2 / 60 / 60 / 24  # Seconds in units of days
    camera = data.draw(tess_st.cameras())
    ccd = data.draw(tess_st.ccds())

    frame_type = data.draw(orm_st.frame_types())
    orbit = data.draw(orm_st.orbits())
    db.add(orbit)
    db.add(frame_type)
    db.flush()

    run_path = directory / pathlib.Path(
        f"orbit-{orbit.orbit_number}", "ffi", "run"
    )
    run_path.mkdir(parents=True)

    subpath = pathlib.Path(
        f"orbit-{orbit.orbit_number}", "ffi", f"cam{camera}", f"ccd{ccd}", "LC"
    )

    quality_flag_path = run_path / pathlib.Path(
        f"cam{camera}ccd{ccd}_qflag.txt"
    )
    tic_catalog_path = run_path / pathlib.Path(
        f"catalog_{orbit.orbit_number}_{camera}_{ccd}_full.txt"
    )

    lightcurve_dump_path = directory / subpath
    lightcurve_dump_path.mkdir(parents=True)

    background_type = data.draw(
        orm_st.lightcurve_types(name=st.just("Background"))
    )
    magnitude_types = [
        data.draw(orm_st.lightcurve_types(name=st.just("RawMagnitude"))),
        data.draw(orm_st.lightcurve_types(name=st.just("KSPMagnitude"))),
    ]

    background_aperture = data.draw(
        orm_st.apertures(name=st.just("BackgroundAperture"))
    )
    photometric_apertures = [
        data.draw(
            orm_st.apertures(
                name=st.just(f"Aperture_00{i}"),
                star_radius=st.just(float(i)),
                inner_radius=st.just(float(i)),
                outer_radius=st.just(float(i)),
            )
        )
        for i in range(1, 6)
    ]
    db.add(background_type)
    db.add(background_aperture)
    db.add_all(magnitude_types)
    db.add_all(photometric_apertures)

    note(f"PUSHING {photometric_apertures}")
    quality_flags = []
    for ith, cadence in enumerate(cadences):
        frame = data.draw(
            orm_st.frames(
                cadence=st.just(cadence),
                camera=st.just(camera),
                exp_time=st.just(exposure_time),
                orbit=st.just(orbit),
                frame_type=st.just(frame_type),
            )
        )
        frame.file_path = f"{frame.cadence}_{frame.file_path}"
        db.add(frame)
        quality_flags.append((cadence, int(frame.quality_bit)))

    # Generate tic parameters
    tic_parameters = data.draw(
        st.lists(tess_st.tic_parameters(), min_size=1, max_size=1)
    )
    _write_tic_catalog(tic_catalog_path, tic_parameters)
    _write_quality_flag_file(quality_flag_path, quality_flags)

    for parameters in tic_parameters:
        simulate_h5_file(
            data,
            lightcurve_dump_path,
            background_aperture.name,
            background_type.name,
            [ap.name for ap in photometric_apertures],
            ["RawMagnitude", "KSPMagnitude"],
            tic_id=st.just(parameters["tic_id"]),
        )
    db.flush()
    return run_path, lightcurve_dump_path
