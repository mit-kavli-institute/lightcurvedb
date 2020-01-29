from hypothesis import given
from lightcurvedb.core.base_model import QLPDataSubType
from lightcurvedb.models.frame import FrameType
from lightcurvedb.models.lightcurve import LightcurveType
from .fixtures import db_conn
from .factories import frame_type as frame_type_st, lightcurve_type as lightcurve_type_st

@given(frame_type_st())
def test_frame_type_creation(frame_type):
    # assert that there are corresponding joined table inheritances
    assert frame_type.subtype == FrameType.__tablename__


@given(frame_type_st())
def test_frame_type_insertion(db_conn, frame_type):
    try:
        db_conn.session.begin_nested()
        db_conn.add(frame_type)
        db_conn.commit()
        q = db_conn.session.query(FrameType).get(frame_type.id)
        assert q is not None

        polymorphic_q = db_conn.session.query(QLPDataSubType).get(frame_type.id)
        assert polymorphic_q is not None
        assert polymorphic_q.subtype == FrameType.__tablename__
        db_conn.session.rollback()
    except Exception as e:
        print('Could not add {} due to {}'.format(frame_type, e))
        # Fall apart
        db_conn.session.rollback()
        assert False is True

@given(lightcurve_type_st())
def test_lightcurve_type_creation(lightcurve_type):
    assert lightcurve_type.subtype == LightcurveType.__tablename__

@given(lightcurve_type_st())
def test_lightcurve_type_insertion(db_conn, lightcurve_type):
    db_conn.session.begin_nested()
    db_conn.add(lightcurve_type)
    db_conn.commit()
    q = db_conn.session.query(LightcurveType).get(lightcurve_type.id)
    assert q is not None

    polymorphic_q = db_conn.session.query(QLPDataSubType).get(lightcurve_type.id)
    assert polymorphic_q is not None
    assert polymorphic_q.subtype == LightcurveType.__tablename__
    db_conn.session.rollback()