import pathlib
from tempfile import TemporaryDirectory

import numpy as np
from astropy.io import fits
from astropy.time import Time
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from hypothesis.extra import numpy as np_st

from lightcurvedb.core.ingestors.frames import from_fits, ingest_directory
from lightcurvedb.models import Frame

from .strategies import ingestion, orm


def _simulate_fits(data, directory):
    header = data.draw(ingestion.ffi_headers())
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


# We rollback all changes to remote db
no_scope_check = HealthCheck.function_scoped_fixture


@settings(suppress_health_check=[no_scope_check])
@given(orm.database(), orm.frame_types(), orm.orbits(), orm.frames())
def test_frame_insertion(database, frame_type, orbit, frame):
    with database as db:
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


@given(st.data())
def test_from_fits(tempdir, data):
    path, header = _simulate_fits(data, tempdir)
    frame = from_fits(path)

    assert header["INT_TIME"] == frame.cadence_type
    assert header["CAM"] == frame.camera
    assert np.isclose(header["TIME"], frame.gps_time)
    assert np.isclose(header["STARTTJD"], frame.start_tjd)
    assert np.isclose(header["MIDTJD"], frame.mid_tjd)
    assert np.isclose(header["EXPTIME"], frame.exp_time)
    assert header["QUAL_BIT"] == frame.quality_bit
    assert str(path) == str(frame.file_path)


@given(orm.database(), orm.frame_types(), st.data())
def test_frame_ingestion(database, frame_type, data):
    with database as db, TemporaryDirectory() as tempdir:
        db.add(frame_type)
        db.flush()
        file_path, ffi_kwargs = _simulate_fits(data, pathlib.Path(tempdir))
        frames = ingest_directory(
            db, frame_type, pathlib.Path(tempdir), "*.fits"
        )

        q = db.query(Frame).filter_by(file_path=file_path).count()
        assert q == 1
        q = db.query(Frame.orbit_id).filter_by(file_path=file_path).first()[0]
        assert q == frames[0].orbit.id


# @given(orm.database(), st.data())
# def test_new_orbit_cli(database, data, clirunner):
#     # Simulate new frames
#     try:
#         with database as db, TemporaryDirectory() as tempdir:
#             frame_type = data.draw(orm.frame_types())
#             db.add(frame_type)
#             db.commit()
#
#             # Simulate POC delivery
#
#             result = clirunner.invoke(
#                 ingest_frames,
#                 [
#                     tempdir
#                 ]
#             )
#     except Exception as e:
#         # Complete catch, we want to try keep the test
#         #database as clean as possible
#         with database as db:
#             opts = {"synchronize_session": False}
#             db.query(Frame).delete(**opts)
#             db.query(FrameType).delete(**opts)
#             db.query(Orbit).delete(**opts)
#             db.commit()
