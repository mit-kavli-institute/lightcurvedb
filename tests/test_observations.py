from hypothesis import strategies as st
from hypothesis import given, note, assume
from lightcurvedb.models import Orbit, Lightcurve, Observation
from random import sample
from .fixtures import db_conn, clear_all
from .factories import observation as observation_st, lightcurve as lightcurve_st, orbit as orbit_st


@given(observation_st())
def test_observation_instantiation(observation):
    assert observation is not None
    assert observation.tic_id is not None
    assert isinstance(observation.tic_id, int)
    assert observation.camera <= 4 and observation.camera >= 1
    assert observation.ccd <= 4 and observation.ccd >= 1
    assert isinstance(observation.orbit, Orbit)


@given(observation_st())
def test_observation_insertions(db_conn, observation):
    try:
        db_conn.session.begin_nested()
        db_conn.add(observation)
        db_conn.commit()

        check = db_conn.query(Observation).get((
            observation.tic_id,
            observation.camera,
            observation.ccd,
            observation.orbit.id
        ))

        assert check.tic_id == observation.tic_id
        assert check.camera == observation.camera
        assert check.ccd == observation.ccd
        assert check.orbit == observation.orbit

        db_conn.session.rollback()
    except Exception:
        db_conn.session.rollback()
        raise
    finally:
        db_conn.session.rollback()
        for q in clear_all():
            db_conn.session.execute(q)
        db_conn.commit()

@given(
    st.lists(
        lightcurve_st(), unique_by=lambda l: l.tic_id,
        min_size=2
    ),
    orbit_st(),
    orbit_st())
def test_lightcurve_retrieval(db_conn, lightcurves, orbit1, orbit2):

    subsample = sample(lightcurves, len(lightcurves) - 1)
    for lc in lightcurves[1:]:
        lc.aperture = lightcurves[0].aperture
        lc.lightcurve_type = lightcurves[0].lightcurve_type

    try:
        orbit2.orbit_number = orbit1.orbit_number + 1
        assert orbit2.orbit_number != orbit1.orbit_number
        db_conn.session.add(orbit1)
        db_conn.session.add(orbit2)
        db_conn.session.add_all(lightcurves)

        for lc in lightcurves:
            db_conn.add(
                Observation(
                    tic_id=lc.tic_id,
                    camera=1,
                    ccd=1,
                    orbit=orbit1
                )
            )

        for lc in subsample:
            db_conn.add(
                Observation(
                    tic_id=lc.tic_id,
                    camera=1,
                    ccd=1,
                    orbit=orbit2
                )
            )
        db_conn.commit()

        orbit1_lcs_check = db_conn.lightcurves_by_observation(
            orbit1
        ).all()
        assert len(orbit1_lcs_check) == len(lightcurves)

        orbit2_lcs_check = db_conn.lightcurves_by_observation(
            orbit2.orbit_number
        ).all()
        assert len(orbit2_lcs_check) == len(subsample)
        db_conn.session.rollback()
    except Exception:
        db_conn.session.rollback()
        raise
    finally:
        for q in clear_all():
            db_conn.session.execute(q)
        db_conn.commit()
