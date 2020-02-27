from hypothesis import strategies as st
from hypothesis import given, note
from lightcurvedb.models.lightcurve import Lightcurve

from .fixtures import db_conn
from .factories import lightcurve as lightcurve_st


@given(lightcurve_st())
def test_retrieval_of_full_lightcurve(db_conn, lightcurve):
    try:
        db_conn.session.begin_nested()
        db_conn.add(lightcurve)

        db_conn.commit()
        note(lightcurve.id)

        lc_obj = db_conn.get_lightcurve(
            lightcurve.tic_id,
            lightcurve.lightcurve_type,
            lightcurve.aperture,
            lightcurve.cadence_type
        )

        assert lc_obj.id == lightcurve.id
        db_conn.session.rollback()
    except:
        note(lightcurve)
        db_conn.session.rollback()
        raise

@given(st.lists(lightcurve_st(), unique_by=lambda l: l.id))
def test_lightcurve_query(db_conn, lightcurves):
    try:
        db_conn.session.begin_nested()
        db_conn.session.add_all(lightcurves)

        ids = db_conn.query_lightcurves(
            cadence_types=[l.cadence_type for l in lightcurves]
            ).value(Lightcurve.id)

        note(ids)
        if ids is None:
            assert len(lightcurves) == 0
        elif len(lightcurves) == 1:
            assert ids == lightcurves[0].id
        else:
            assert set(ids) == set(l.id for l in lightcurves)

        db_conn.session.rollback()
    except:
        note(lightcurves)
        db_conn.session.rollback()
        raise


@given(lightcurve_st())
def test_orbit_lightcurve_instantiation(db_conn, lightcurve):
    db_conn.session.begin_nested()
    db_conn.add(lightcurve)
    note(lightcurve)
    length = len(lightcurve)
    attrs = [
        lightcurve.cadences,
        lightcurve.bjd,
        lightcurve.values,
        lightcurve.errors,
        lightcurve.x_centroids,
        lightcurve.y_centroids,
        lightcurve.quality_flags,
    ]

    for attr in attrs:
        note(attr)

    assert all(len(attr) == length for attr in attrs)

    db_conn.session.rollback()
