from datetime import datetime

from itertools import combinations

import numpy as np
from hypothesis import assume, given, note, settings
from hypothesis import strategies as st

from lightcurvedb.models import CameraQuaternion
from lightcurvedb.util.constants import TESS_FIRST_LIGHT
from lightcurvedb.util.decorators import suppress_warnings

from .factories import quaternion
from .fixtures import clear_all, db_conn

GPS_EPOCH = (datetime.now() - datetime(1980, 1, 6)).total_seconds()


@given(quaternion(missing=False))
def test_full_quaternion(q):
    assume(sum(q) <= 1.0)
    q1, q2, q3, q4 = q

    camera_quat = CameraQuaternion(q1=q1, q2=q2, q3=q3, q4=q4)
    assert camera_quat is not None

    camera_quat = CameraQuaternion(w=q1, x=q2, y=q3, z=q4)
    assert camera_quat is not None


@given(
    st.floats(
        min_value=0.0,
        max_value=GPS_EPOCH,
        allow_nan=False,
        allow_infinity=False,
    )
)
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


@settings(deadline=None, max_examples=10)
@given(
    quaternion(missing=False),
    st.integers(min_value=1, max_value=4),
    st.datetimes(min_value=TESS_FIRST_LIGHT, max_value=datetime.now()),
)
def test_psql_gps(db_conn, q, camera, date):
    q1, q2, q3, q4 = q

    camera_quat = CameraQuaternion(
        q1=q1, q2=q2, q3=q3, q4=q4, camera=camera, date=date
    )
    gps_ref = camera_quat.gps_time

    with db_conn as db:
        try:
            db.add(camera_quat)
            db.commit()

            gps_time = (
                db.query(CameraQuaternion.gps_time)
                .filter(CameraQuaternion.id == camera_quat.id)
                .one()[0]
            )
        finally:
            db.rollback()
            clear_all(db)

    diff = gps_time - gps_ref.value

    note("diff {0}".format(diff))
    assert np.isclose(gps_time, gps_ref.value)
