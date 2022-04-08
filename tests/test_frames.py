import pathlib
import numpy as np
from astropy.io import fits
from hypothesis import given, settings, HealthCheck, assume, strategies as st
from hypothesis.extra import numpy as np_st
from lightcurvedb.models import Orbit, FrameType, Frame
from lightcurvedb.cli.new_orbit import ingest_frames
from .conftest import db
from .strategies import orm, ingestion


def _simulate_fits(data, directory):
    header = data.draw(ingestion.ffi_headers())
    ffi_data = data.draw(
        np_st.arrays(
            np.int32,
            (header["NAXIS1"], header["NAXIS2"])
        )
    )
    cam = header["CAM"]
    cadence = header["CADENCE"]
    filename = pathlib.Path(
        f"TESS2022123456_ffi_cam_{cam}_{cadence}"
    )

    hdu = fits.PrimaryHDU(ffi_data)
    hdr = hdu.header
    hdul = fits.HDUList([hdu])

    for key, value in header.items():
        hdr[key] = value
    hdul.writeto(directory / filename, overwrite=True)
    return directory / filename


# We rollback all changes to remote db
no_scope_check = HealthCheck.function_scoped_fixture

@settings(suppress_health_check=[no_scope_check])
@given(orm.frame_types(), orm.orbits(), orm.frames())
def test_frame_insertion(db, frame_type, orbit, frame):
    db.add(orbit)
    db.add(frame_type)
    db.flush()
    frame.frame_type = frame_type
    frame.orbit = orbit
    db.add(frame)

    # Tautological query of frame
    db.query(Frame).filter_by(id=frame.id).count() == 1

    # Obtain frame by type
    db.query(Frame).filter(Frame.frame_type == frame_type).count() == 1

    # Obtain frame by orbit
    db.query(Frame).filter(Frame.orbit == orbit).count() == 1

    db.rollback()


@settings(suppress_health_check=[no_scope_check])
@given(st.data())
def test_frame_ingestion(db, tempdir, data):
    filepath = _simulate_fits(data, tempdir)
