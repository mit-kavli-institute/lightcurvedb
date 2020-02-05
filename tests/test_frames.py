import pytest
from hypothesis import given, note
from hypothesis import strategies as st
from sqlalchemy.exc import IntegrityError
from psycopg2.errors import NumericValueOutOfRange
from lightcurvedb.models.frame import Frame

from .fixtures import db_conn
from .factories import frame as frame_st

@given(frame_st())
def test_frame_retrieval(db_conn, frame):
    try:
        db_conn.session.begin_nested()
        db_conn.add(frame)
        db_conn.commit()
        q = db_conn.session.query(Frame).get(frame.id)
        assert q is not None
    except Exception as e:
        note('Could not add {} due to {}'.format(frame, e))
        db_conn.session.rollback()
        raise
    db_conn.session.rollback()

@given(frame_st(camera=st.integers().filter(lambda i: (i < 0 and i > -32766) or (i > 4 and i < 32767))))
def test_frame_physical_camera_constraint(db_conn, frame):
    try:
        db_conn.session.begin_nested()
        db_conn.add(frame)
        with pytest.raises(IntegrityError):
            db_conn.commit()
        db_conn.session.rollback()
    except Exception as e:
        note('Could not add {} due to {}'.format(frame, e))
        db_conn.session.rollback()
        raise
    db_conn.session.rollback()

@given(frame_st(ccd=st.integers().filter(lambda i: (i < 0 and i > -32766) or (i > 4 and i < 32767))))
def test_frame_physical_ccd_constraint(db_conn, frame):
    try:
        db_conn.session.begin_nested()
        db_conn.add(frame)
        with pytest.raises(IntegrityError):
            db_conn.commit()
        db_conn.session.rollback()
    except Exception as e:
        note('Could not add {} due to {}'.format(frame, e))
        db_conn.session.rollback()
        raise
    db_conn.session.rollback()

