from hypothesis import given
from lightcurvedb.models.frame import Frame

from .fixtures import db_conn, clean_db
from .factories import frame as frame_st

@given(frame_st())
@clean_db
def test_frame_retrieval(db_conn, frame):
    db_conn.add(frame.frame_type)
    db_conn.add(frame.orbit)
    db_conn.add(frame)
    db_conn.conn_commit()
    q = db_conn.session.query(Frame).get(frame.id)
    assert q is not None