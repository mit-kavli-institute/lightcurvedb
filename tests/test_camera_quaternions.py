import tempfile

from hypothesis import HealthCheck, given, note, settings
from hypothesis import strategies as st

from lightcurvedb.core.ingestors.camera_quaternions import ingest_quat_file
from lightcurvedb.models.camera_quaternion import (
    CameraQuaternion,
    get_utc_time,
)

from .strategies import ingestion, orm

no_scope_check = HealthCheck.function_scoped_fixture


@settings(deadline=None, suppress_health_check=[no_scope_check])
@given(orm.database(), st.data())
def test_camera_quaternion_ingest(database, data):
    with database as db:
        with tempfile.TemporaryDirectory() as tempdir:
            quat_path, camera, quaternions = ingestion.simulate_hk_file(
                data, tempdir
            )
            ingest_quat_file(db, quat_path)
            db.flush()
            template = db.query(CameraQuaternion)
            for quaternion in quaternions:
                utc_time = get_utc_time(quaternion.gps_time)
                q = template.filter(
                    CameraQuaternion.date == utc_time.datetime,
                    CameraQuaternion.camera == camera,
                )
                orm_quaternion = q.one()
                for quatfield in ("q1", "q2", "q3", "q4"):
                    ref = getattr(quaternion, quatfield)
                    remote = getattr(orm_quaternion, quatfield)
                    note(f"{quatfield}, {ref}, {remote}")
                    assert ref == remote