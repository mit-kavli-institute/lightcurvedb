"""
Generate data and push to a static file to test for ingestion correctness
"""
import itertools
import pathlib
from collections import namedtuple

import numpy as np
import pandas as pd
from astropy.io import fits
from astropy.time import Time
from h5py import File as H5File
from hypothesis import strategies as st
from hypothesis.extra import numpy as np_st

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

    length = data.draw(overrides.get("length", st.just(20)))

    background_lc = pd.DataFrame(
        data.draw(st.lists(lightpoints(), max_size=length, min_size=length))
    )
    data_gen_ref = {
        "tic_id": tic_id,
        "background": background_lc,
        "photometry": {},
    }

    with H5File(directory / filename, "w") as h5:
        lc = h5.create_group("LightCurve")
        lc.create_dataset("Cadence", data=background_lc.cadence)
        lc.create_dataset("BJD", data=background_lc.barycentric_julian_date)
        lc.create_dataset("X", data=background_lc.x_centroid)
        lc.create_dataset("Y", data=background_lc.y_centroid)

        prod = itertools.product(apertures, lightcurve_types)

        photometry = lc.create_group("AperturePhotometry")
        for ap, lc_t in prod:
            api = photometry.create_group(ap)

            lc = pd.DataFrame(
                data.draw(
                    st.lists(
                        shallow_lightpoints(), max_size=length, min_size=length
                    )
                )
            )
            data_gen_ref["photometry"][(ap, lc_t)] = lc
            api.create_dataset(lc_t, data=lc.data)
            api.create_dataset(f"{lc_t}Error", data=lc.error)
            api.create_dataset("X", data=lc.x_centroid)
            api.create_dataset("Y", data=lc.y_centroid)
            api.create_dataset("QualityFlag", data=lc.quality_flag)
    return directory / filename, data_gen_ref
