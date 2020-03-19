from hypothesis import strategies as st
from hypothesis import given, note
from lightcurvedb.models.lightcurve import Lightcurve

from .fixtures import db_conn
from .factories import lightcurve as lightcurve_st



@given(st.lists(lightcurve_st(), unique_by=lambda l: l.id))
def test_lightcurve_query(db_conn, lightcurves):
    try:
        db_conn.session.begin_nested()
        db_conn.session.add_all(lightcurves)
        db_conn.session.commit()

        results = db_conn.query_lightcurves(
            cadence_types=[l.cadence_type for l in lightcurves]
            )

        ids = [result.id for result in results]

        note(str(db_conn.query_lightcurves(
            cadence_types=[l.cadence_type for l in lightcurves])
        ))

        note(ids)
        assert set(ids) == set(l.id for l in lightcurves)

        db_conn.session.rollback()
    except:
        note(lightcurves)
        db_conn.session.rollback()
        raise


@given(st.lists(lightcurve_st(), unique_by=(lambda l: l.id, lambda l: l.aperture.id, lambda l: l.lightcurve_type.id)))
def test_load_from_db(db_conn, lightcurves):
    try:
        db_conn.session.begin_nested()
        db_conn.session.add_all(lightcurves)
        db_conn.session.commit()
        ids = db_conn.load_from_db(
            cadence_types=[l.cadence_type for l in lightcurves]
        )
        note([l.cadence_type for l in lightcurves])

        if ids is None:
            ids = []
        else:
            ids = [l.id for l in ids]
        note(ids)
        assert set(ids) == set(l.id for l in lightcurves)

        db_conn.session.rollback()
    except:
        note(lightcurves)
        db_conn.session.rollback()
        raise


@given(st.lists(lightcurve_st(tic_id=st.just(10), cadence_type=st.just(30)), unique_by=(lambda l: l.id, lambda l: l.aperture.id, lambda l: l.lightcurve_type.id)))
def test_get_lightcurve_w_model(db_conn, lightcurves):
    try:
        db_conn.session.begin_nested()
        db_conn.session.add_all(lightcurves)
        db_conn.commit()

        # Query by model
        for lightcurve in lightcurves:
            q = db_conn.get_lightcurve(
                lightcurve.tic_id,
                lightcurve.lightcurve_type,
                lightcurve.aperture,
                lightcurve.cadence_type,
                resolve=False
            )

            note(str(q))
            result = q.one()

            assert result is not None

        db_conn.session.rollback()
    except:
        db_conn.session.rollback()
        raise


@given(st.lists(lightcurve_st(tic_id=st.just(10), cadence_type=st.just(30)), unique_by=(lambda l: l.id, lambda l: l.aperture.id, lambda l: l.lightcurve_type.id)))
def test_get_lightcurve_w_str(db_conn, lightcurves):
    try:
        db_conn.session.begin_nested()
        db_conn.session.add_all(lightcurves)
        db_conn.commit()

        # Query by model
        for lightcurve in lightcurves:
            # Query by name
            q = db_conn.get_lightcurve(
                lightcurve.tic_id,
                lightcurve.lightcurve_type.name,
                lightcurve.aperture.name,
                lightcurve.cadence_type,
                resolve=False
            )

            note(q)
            result = q.one()

            assert result is not None

        db_conn.session.rollback()
    except:
        db_conn.session.rollback()
        raise
