from hypothesis import strategies as st, given, note, assume
from lightcurvedb.models import CameraQuaternion
from itertools import combinations
from .factories import quaternion


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

@given(quaternion(missing=True))
def test_missing_quaternion(q):
    assume(sum(q) <= 1.0)
    standard_keys = ['w', 'x', 'y', 'z']
    q_keys = ['q0', 'q1', 'q2', 'q3']

    for params in combinations(standard_keys, 3):
        keywords = dict(zip(params, q))
        camera_quat = CameraQuaternion(**keywords)
        assert camera_quat is not None

    for params in combinations(q_keys, 3):
        keywords = dict(zip(params, q))
        camera_quat = CameraQuaternion(**keywords)
        assert camera_quat is not None
