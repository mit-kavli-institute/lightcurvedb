from hypothesis import given
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
        print('Could not add {} due to {}'.format(frame, e))
        db_conn.session.rollback()
        raise
    db_conn.session.rollback()