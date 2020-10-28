from hypothesis import strategies as st, given, note, assume
from lightcurvedb.models import CameraQuaternion
from lightcurvedb.util.decorators import suppress_warnings
from itertools import combinations
from datetime import datetime
from .factories import quaternion

GPS_EPOCH = (datetime.now() - datetime(1980, 1, 6)).total_seconds()


@given(quaternion(missing=False))
def test_full_quaternion(q):
    assume(sum(q) <= 1.0)
    q0, q1, q2, q3 = q

    camera_quat = CameraQuaternion(
        q0=q0,
        q1=q1,
        q2=q2,
        q3=q3
    )
    assert camera_quat is not None

    camera_quat = CameraQuaternion(
        w=q0,
        x=q1,
        y=q2,
        z=q3
    )
    assert camera_quat is not None


@given(st.floats(min_value=0.0, max_value=GPS_EPOCH, allow_nan=False, allow_infinity=False))
def test_gps_time_assignment(gps_time):
    camera_quat = CameraQuaternion(gps_time=gps_time)
    assert camera_quat.date is not None


@suppress_warnings
@given(st.datetimes(max_value=datetime.now()))
def test_datetime_assignment(date):
    camera_quat = CameraQuaternion(date=date)
    note(date)
    assert camera_quat.gps_time is not None


@suppress_warnings
@given(st.datetimes(max_value=datetime.now()))
def test_datetime_equivalency(date):
    camera_quat = CameraQuaternion(date=date)
    gps_time = camera_quat.gps_time
    camera_quat.gps_time = gps_time
    assert camera_quat.date == date

