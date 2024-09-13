import pathlib
from tempfile import TemporaryDirectory

import numpy as np
import sqlalchemy as sa
from click.testing import CliRunner
from hypothesis import HealthCheck, given, note, settings
from hypothesis import strategies as st

from lightcurvedb.cli import lcdbcli
from lightcurvedb.core.ingestors.frames import from_fits_header
from lightcurvedb.models import Frame, FrameType
from lightcurvedb.models.frame import FRAME_MAPPER_LOOKUP

from .strategies import ingestion, orm

# We rollback all changes to remote db
no_scope_check = HealthCheck.function_scoped_fixture


@settings(deadline=None, suppress_health_check=[no_scope_check])
@given(orm.frame_types(), orm.orbits(), orm.frames())
def test_frame_insertion(db_session, frame_type, orbit, frame):
    with db_session as db:
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


@settings(
    suppress_health_check=[HealthCheck.function_scoped_fixture], deadline=None
)
@given(st.data())
def test_from_fits_header(tempdir, data):
    files, headers = ingestion.simulate_tica_fits(data, tempdir)

    for path, header in zip(files, headers):
        frame = from_fits_header(header)
        frame.file_path = path

        for attr, val in header.items():
            if hasattr(Frame, attr):
                to_check = getattr(frame, attr)
                if isinstance(val, float) and isinstance(to_check, float):
                    assert np.isclose(val, to_check)
                else:
                    assert val == getattr(frame, attr)

        assert header["INT_TIME"] == frame.cadence_type
        assert header["CAM"] == frame.camera
        assert np.isclose(header["TIME"], frame.gps_time)
        assert np.isclose(header["STARTTJD"], frame.start_tjd)
        assert np.isclose(header["MIDTJD"], frame.mid_tjd)
        assert np.isclose(header["EXPTIME"], frame.exposure_time)
        assert header["QUAL_BIT"] == frame.quality_bit
        assert str(path) == str(frame.file_path)


@settings(
    deadline=None,
    suppress_health_check=[
        HealthCheck.too_slow,
        HealthCheck.function_scoped_fixture,
    ],
)
@given(st.data())
def test_new_orbit_cli(db_session, data):
    clirunner = CliRunner()
    # Simulate new frames
    with db_session as db, TemporaryDirectory() as tempdir:
        try:
            frame_type = data.draw(orm.frame_types())
            raw_path = pathlib.Path(tempdir)

            db.add(frame_type)
            db.commit()

            # Simulate TICA delivery
            paths, headers = ingestion.simulate_tica_fits(data, raw_path)

            # Invoke Command Line
            note(f"Ingesting {tempdir}")
            result = clirunner.invoke(
                lcdbcli,
                [
                    "ingest-frames",
                    tempdir,
                    "--frame-type-name",
                    frame_type.name,
                ],
                catch_exceptions=False,
            )
            note(str(result))
            note(result.stdout_bytes)
            note(str(result.stderr_bytes))

            for path, header in zip(paths, headers):
                q = db.query(Frame).where(Frame.file_path == path)
                assert q.count() == 1
                remote_frame = db.scalar(q)
                for key, model_attr in FRAME_MAPPER_LOOKUP.items():
                    check = header[key]
                    remote_value = getattr(remote_frame, model_attr)
                    assert remote_value == check

                assert remote_frame.orbit.orbit_number == header["ORBIT_ID"]
                assert remote_frame.stray_light is not None

        finally:
            db.rollback()
            db.execute(sa.delete(Frame))
            db.execute(sa.delete(FrameType))
            db.commit()
