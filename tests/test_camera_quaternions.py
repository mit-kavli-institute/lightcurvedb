import pathlib
import tempfile
from .strategies import ingestion, tess as tess_st
from hypothesis import given, strategies as st, HealthCheck, settings, note
from lightcurvedb.models import CameraQuaternion
from lightcurvedb.models.camera_quaternion import CameraQuaternion, get_utc_time
from lightcurvedb.core.ingestors.camera_quaternion import ingest_quat_file

no_scope_check = HealthCheck.function_scoped_fixture

def _simulate_file(directory, camera, data, formatter=str):
    filename = pathlib.Path(f"cam{camera}_quat.txt")
    path = pathlib.Path(directory) / filename

    with open(path, "wt") as fout:
        for row in data:
            line = " ".join(map(formatter, row))
            fout.write(line)
            fout.write("\n")
    return path

@settings(deadline=None, suppress_health_check=[no_scope_check])
@given(
    st.lists(
        ingestion.camera_quaternions(),
        unique_by=lambda cq: cq[0]
    ),
    tess_st.cameras()
)
def test_camera_quaternion_ingest(db, quaternions, camera):
    db.query(CameraQuaternion).delete(synchronize_session=False)
    with tempfile.TemporaryDirectory() as tmpdir:
        quat_path = _simulate_file(tmpdir, camera, quaternions)
        ingest_quat_file(
            db,
            quat_path
        )
        db.flush()
        template = db.query(CameraQuaternion)
        for quaternion in quaternions:
            utc_time = get_utc_time(quaternion.gps_time)
            q = (
                template
                .filter(
                    CameraQuaternion.date == utc_time.datetime,
                    CameraQuaternion.camera == camera
                )
            )
            orm_quaternion = q.one()
            for quatfield in ("q1", "q2", "q3", "q4"):
                ref = getattr(quaternion, quatfield)
                remote = getattr(orm_quaternion, quatfield)
                note(f"{quatfield}, {ref}, {remote}")
                assert ref == remote
    db.rollback()
