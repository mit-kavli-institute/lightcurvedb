from hypothesis import strategies as st, given, note, settings
from hypothesis.extra.numpy import arrays
from lightcurvedb.models import Lightcurve, Lightpoint
from .factories import lightcurve, lightcurve_type, aperture, lightpoint, frame_type, frame, orbit
from .constants import PSQL_INT_MAX
from .fixtures import db_conn, clear_all
from itertools import combinations
import numpy as np


NP_ATTRS = {
    'cadences',
    'bjd',
    'barycentric_julian_date',
    'values',
    'mag',
    'errors',
    'x_centroids',
    'y_centroids',
    'quality_flags'
}


@given(st.integers(min_value=1), aperture(), lightcurve_type())
def test_lightcurve_instantiation(tic, aperture, lc_type):
    lc = Lightcurve(tic_id=tic, aperture=aperture, lightcurve_type=lc_type)
    assert lc.tic_id == tic
    assert lc.aperture == aperture
    assert lc.type == lc_type


@given(
    st.builds(
        Lightpoint,
        lightcurve_id=st.integers(min_value=1, max_value=99999),
        cadence=st.integers(min_value=0, max_value=PSQL_INT_MAX),
        bjd=st.floats(),
        data=st.floats(),
        error=st.floats(),
        x=st.floats(),
        y=st.floats(),
        quality_flag=st.integers(min_value=0, max_value=PSQL_INT_MAX),
    ),
    st.integers(min_value=1),
    aperture(),
    lightcurve_type(),
)
def test_lightpoint_collection_append(lp, tic, aperture, lc_type):
    lc = Lightcurve(tic_id=tic, aperture=aperture, lightcurve_type=lc_type)

    assert len(lc) == 0

    lc.lightpoints.append(lp)

    assert len(lc) == 1
    assert lc.cadences[0] == lp.cadence

    for attr in NP_ATTRS:
        assert len(lc[attr]) == 1
        assert isinstance(lc[attr], np.ndarray)

    if np.isnan(lp.bjd):
        assert np.isnan(lc.bjd[0])
    else:
        assert lc.bjd[0] == lp.bjd


@given(lightcurve(), st.data())
def test_lightpoint_mass_assignment(lightcurve, data):
    lightcurve.id = 1
    lightpoints = data.draw(
        st.lists(
            lightpoint(id_=st.just(1)),
            unique_by=lambda lp: lp.cadence
        )
    )

    lightcurve.lightpoints = lightpoints
    assert len(lightcurve) == len(lightpoints)


@given(
    lightcurve(),
    st.lists(
        lightpoint(),
        min_size=1,
        unique_by=lambda lp:lp.cadence
    ),
    st.data()
)
def test_iterable_keying(lightcurve, lightpoints, data):
    lightcurve.lightpoints = lightpoints
    cadences = lightcurve.cadences

    idx = data.draw(
        st.lists(
            st.sampled_from(cadences),
            max_size=len(cadences)
        )
    )

    sliced = lightcurve.lightpoints[idx]
    assert len(sliced) <= len(idx)
    for lp in sliced:
        assert lp.cadence in idx


@given(
    lightcurve(),
    st.lists(
        lightpoint(),
        min_size=1,
        unique_by=lambda lp:lp.cadence
    ),
    st.data()
)
def test_subslice_assignment(lightcurve, lightpoints, data):
    lightcurve.lightpoints = lightpoints
    cadences = lightcurve.cadences

    idx = data.draw(
        st.lists(
            st.sampled_from(cadences),
            max_size=len(cadences)
        )
    )

    sliced = lightcurve.lightpoints[idx]

    float_columns = {
        'bjd': float,
        'values': float,
        'errors': float,
        'x_centroids': float,
        'y_centroids': float,
        'quality_flags': int
    }

    for col, type_ in float_columns.items():
        test = list(map(type_, range(len(sliced))))
        sliced[col] = test

        for lp, val in zip(sliced, test):
            note(lp)
            note(val)
            assert lp[col] == val


@given(
    lightcurve(),
    st.lists(
        lightpoint(),
        min_size=1,
        unique_by=lambda lp:lp.cadence
    ),
    st.data()
)
def test_full_assignment(lightcurve, lightpoints, data):
    lightcurve.lightpoints = lightpoints

    float_columns = {
        'bjd': float,
        'values': float,
        'errors': float,
        'x_centroids': float,
        'y_centroids': float,
        'quality_flags': int
    }

    for col, type_ in float_columns.items():
        test = list(map(type_, range(len(lightcurve))))
        lightcurve[col] = test

        for lp, val in zip(lightcurve, test):
            note(lp)
            note(val)
            assert lp[col] == val


@settings(deadline=None)
@given(
    lightcurve(),
    st.lists(
        lightpoint(),
        min_size=1,
        unique_by=lambda lp:lp.cadence
    )
)
def test_lightpoint_q(db_conn, lightcurve, lightpoints):

    # Generate frames that fit the lightpoints
    cadences = [lp.cadence for lp in lightpoints]
    note(cadences)

    lightcurve.id = 1

    with db_conn as db:
        try:
            db.add(lightcurve)
            db.commit()

            for lp in lightpoints:
                lp.lightcurve_id = lightcurve.id

            db.session.add_all(lightpoints)
            db.commit()

            test = lightcurve.lightpoints_by_cadence_q(
                db.query(Lightpoint.cadence).filter(
                    Lightpoint.lightcurve_id == lightcurve.id
                ).subquery()
            )

            assert all(lp.cadence in cadences for lp in test)
            assert len(test) == len(cadences)

        finally:
            db.rollback()
            clear_all(db)


@settings(deadline=None)
@given(
    lightcurve(),
    orbit(),
    st.lists(
        lightpoint(),
        min_size=1,
        unique_by=lambda lp:lp.cadence
    ),
    frame_type(),
    st.data(),
)
def test_point_dynamic_link(db_conn, lightcurve, orbit, lightpoints, frame_type, data):

    # Generate frames that fit the lightpoints
    cadences = [lp.cadence for lp in lightpoints]
    note(cadences)

    frames = []

    for cadence in cadences:
        f = data.draw(frame(cadence=st.just(cadence)))
        f.frame_type = frame_type
        f.orbit = orbit
        frames.append(f)

    lightcurve.id = 1

    with db_conn as db:
        try:
            db.add(frame_type)
            db.add(orbit)
            db.commit()

            db.session.add_all(frames)
            db.add(lightcurve)
            db.commit()

            for lp in lightpoints:
                lp.lightcurve_id = lightcurve.id

            db.session.add_all(lightpoints)
            db.commit()

            test = lightcurve.lightpoints_by_orbit(
                [frames[0].orbit.orbit_number],
                frame_type.id
            )

            assert all(lp.cadence in cadences for lp in test)

            assert len(test) == len(cadences)

        finally:
            db.rollback()
            clear_all(db)


@settings(deadline=None)
@given(
    lightcurve(),
    st.lists(
        lightpoint(),
        min_size=1,
        unique_by=lambda lp:lp.cadence
    )
)
def test_lightpoint_q_cadence_tracking(db_conn, lightcurve, lightpoints):

    # Generate frames that fit the lightpoints
    cadences = [lp.cadence for lp in lightpoints]
    note(cadences)

    lightcurve.id = 1

    with db_conn as db:
        try:
            db.add(lightcurve)
            db.commit()

            for lp in lightpoints:
                lp.lightcurve_id = lightcurve.id

            db.session.add_all(lightpoints)
            db.commit()

            test = lightcurve.lightpoints_by_cadence_q(
                db.query(Lightpoint.cadence).filter(
                    Lightpoint.lightcurve_id == lightcurve.id
                ).subquery()
            )

            note(test["mag"])
            assert test["mag"] is not None
            assert test["x_centroids"] is not None
            assert test["flux"] is not None

        finally:
            db.rollback()
            clear_all(db)

