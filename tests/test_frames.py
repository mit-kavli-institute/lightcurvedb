import pathlib
from tempfile import TemporaryDirectory

import numpy as np
from hypothesis import HealthCheck, given, note, settings
from hypothesis import strategies as st

from lightcurvedb.cli import lcdbcli
from lightcurvedb.core.ingestors.frames import from_fits, ingest_directory
from lightcurvedb.models import CameraQuaternion, Frame, FrameType, Orbit

from .strategies import ingestion, orm

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
    path, header = ingestion.simulate_fits(data, tempdir)
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
        file_path, ffi_kwargs = ingestion.simulate_fits(
            data, pathlib.Path(tempdir)
        )
        frames = ingest_directory(
            db, frame_type, pathlib.Path(tempdir), "*.fits"
        )

        q = db.query(Frame).filter_by(file_path=file_path).count()
        assert q == 1
        q = db.query(Frame.orbit_id).filter_by(file_path=file_path).first()[0]
        assert q == frames[0].orbit.id


@settings(deadline=None)
@given(orm.database(), st.data())
def test_new_orbit_cli(clirunner, database, data):
    # Simulate new frames
    try:
        with database as db, TemporaryDirectory() as tempdir:
            frame_type = data.draw(orm.frame_types())
            ffi_path = pathlib.Path(tempdir, "ffi_fits")
            hk_path = pathlib.Path(tempdir, "hk")

            ffi_path.mkdir()
            hk_path.mkdir()

            db.add(frame_type)
            db.commit()

            # Simulate POC delivery
            path, header = ingestion.simulate_fits(data, ffi_path)

            quaternions = []
            for cam in (1, 2, 3, 4):
                _, _, camera_quaternions = ingestion.simulate_hk_file(
                    data, hk_path, camera=st.just(cam)
                )
                quaternions.extend(camera_quaternions)

            # Invoke Command Line
            result = clirunner.invoke(
                lcdbcli,
                [
                    "--dbconf",
                    db._config,
                    "ingest-frames",
                    tempdir,
                    "--frame-type-name",
                    frame_type.name,
                ],
                catch_exceptions=False,
            )
            note(result.stdout_bytes)
            assert result.exit_code == 0
            assert db.query(Frame).filter_by(file_path=path).count() == 1
            assert (
                db.query(Orbit)
                .filter_by(orbit_number=header["ORBIT_ID"])
                .count()
                == 1
            )
            assert db.query(CameraQuaternion).count() == len(quaternions)

    finally:
        # Complete catch, we want to try keep the test
        # database as clean as possible
        with database as db:
            opts = {"synchronize_session": False}
            db.query(Frame).delete(**opts)
            db.query(FrameType).delete(**opts)
            db.query(Orbit).delete(**opts)
            db.query(CameraQuaternion).delete(**opts)
            db.commit()
