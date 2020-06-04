from hypothesis import given, note, assume, settings, HealthCheck
from hypothesis import strategies as st
from hypothesis.extra import numpy as np_st
from .factories import lightcurve as lightcurve_st, lightcurve_list, aperture as aperture_st, lightcurve_type as lightcurve_type_st
from .fixtures import db_conn, clear_all
from .util import arr_equal
from lightcurvedb.core.base_model import QLPModel
from lightcurvedb.managers.lightcurve_query import LightcurveManager
from lightcurvedb.models import Lightcurve, LightcurveType
from random import sample
import numpy as np


@settings(deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(st.lists(lightcurve_st(), unique_by=lambda l: l.lightcurve_type))
def test_grouping_by_type(lightcurves):
    types = {lc.type.name for lc in lightcurves}

    for i, lc in enumerate(lightcurves):
        lc.id = i

    manager = LightcurveManager(lightcurves)
    assert len(manager) == len(lightcurves)
    note(types)
    note(manager.searchables)
    note(manager.id_map)
    for type_ in types:
        filtered = set(filter(lambda lc: lc.type.name == type_, lightcurves))
        note('keying by {}'.format(type_))
        result = manager[str(type_)]
        note('result {}'.format(result))

        if isinstance(result, LightcurveManager):
            assert filtered == set(result)
        else:
            assert filtered.pop().id == result.id


@settings(deadline=None, suppress_health_check=[HealthCheck.too_slow, HealthCheck.filter_too_much])
@given(st.lists(lightcurve_st(), min_size=3))
def test_grouping_by_aperture(lightcurves):

    for i, lc in enumerate(lightcurves):
        lc.id = i

    apertures = {lc.aperture.name for lc in lightcurves}

    manager = LightcurveManager(lightcurves)
    assert len(manager) == len(lightcurves)
    note(apertures)
    note(manager.searchables)
    note(manager.id_map)
    for aperture in apertures:
        filtered = set(filter(lambda lc: lc.aperture.name == aperture, lightcurves))
        note('keying by {}'.format(aperture))
        result = manager[aperture]
        note('result {}'.format(result))

        if isinstance(result, LightcurveManager):
            assert filtered == set(result)
        else:
            assert filtered.pop().id == result.id


@given(lightcurve_st())
def test_insert_manager_q(db_conn, lightcurve):
    try:
        db_conn.session.begin_nested()
        lc_type = lightcurve.lightcurve_type
        aperture = lightcurve.aperture
        lightcurve.lightcurve_type = None
        lightcurve.aperture = None
        db_conn.add(lc_type)
        db_conn.add(aperture)
        db_conn.commit()

        assert db_conn.lightcurves.count() == 0
        manager = LightcurveManager([], internal_session=db_conn)
        manager.add(
            lightcurve.tic_id,
            aperture,
            lc_type,
            cadences=lightcurve.cadences,
            bjd=lightcurve.bjd,
            values=lightcurve.values,
            errors=lightcurve.errors,
            x_centroids=lightcurve.x_centroids,
            y_centroids=lightcurve.y_centroids,
            quality_flags=lightcurve.quality_flags
        )

        q = manager.insert_q()

        assert q is not None
        manager.execute()
        db_conn.commit()

        check = db_conn.lightcurves.filter(
            Lightcurve.aperture == lightcurve.aperture,
            Lightcurve.lightcurve_type == lightcurve.lightcurve_type,
            Lightcurve.tic_id == lightcurve.tic_id,
            Lightcurve.cadence_type == lightcurve.cadence_type
        ).scalar is not None

        assert check is not None

        db_conn.session.rollback()
    except Exception:
        db_conn.session.rollback()
        raise
    finally:
        for q in clear_all():
            db_conn.session.execute(q)
        db_conn.commit()

@given(lightcurve_st(with_id=st.integers(min_value=1, max_value=10**9)))
def test_update_manager_q(db_conn, lightcurve):
    try:
        db_conn.session.begin_nested()
        db_conn.session.add(lightcurve)
        db_conn.session.commit()

        lm = LightcurveManager(db_conn.lightcurves.all())

        new_values = np.arange(len(lm))

        lm.update(
            lightcurve.tic_id,
            lightcurve.aperture,
            lightcurve.lightcurve_type,
            values=new_values.tolist()
        )
        note(lm._to_update)
        lm.execute(session=db_conn)
        db_conn.commit()

        assert db_conn.lightcurves.filter(
            Lightcurve.aperture == lightcurve.aperture,
            Lightcurve.lightcurve_type == lightcurve.lightcurve_type,
            Lightcurve.tic_id == lightcurve.tic_id
        ).one().values == new_values
        db_conn.session.rollback()
    except Exception:
        db_conn.session.rollback()
        raise
    finally:
        for q in clear_all():
            db_conn.session.execute(q)
        db_conn.commit()


@settings(deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(lightcurve_st(), lightcurve_st())
def test_upsert_manager_q(db_conn, lc1, lc2):
    assume(lc1.tic_id != lc2.tic_id)
    assert db_conn.lightcurves.count() == 0
    try:

        lc2.aperture = lc1.aperture
        lc2.lightcurve_type = lc1.lightcurve_type
        lc2.cadence_type = lc1.cadence_type

        db_conn.session.begin_nested()
        db_conn.session.add(lc1)
        db_conn.commit()

        lm = LightcurveManager([], internal_session=db_conn)

        new_val = np.arange(len(lc1))

        lm.upsert(
            lc1.tic_id,
            lc1.aperture,
            lc1.lightcurve_type,
            cadences=lc1.cadences,
            bjd=lc1.bjd,
            values=new_val,
            errors=lc1.errors,
            x_centroids=lc1.x_centroids,
            y_centroids=lc1.y_centroids,
            quality_flags=lc1.quality_flags
        )

        lm.upsert(
            lc2.tic_id,
            lc2.aperture,
            lc2.lightcurve_type,
            cadences=lc2.cadences,
            bjd=lc2.bjd,
            values=lc2.values,
            errors=lc2.errors,
            x_centroids=lc2.x_centroids,
            y_centroids=lc2.y_centroids,
            quality_flags=lc2.quality_flags
        )

        note(lm._to_upsert)

        lm.execute()
        db_conn.commit()

        all_lightcurves = db_conn.lightcurves.all()
        assert len(all_lightcurves) == 2

        check = db_conn.lightcurves.filter(
            Lightcurve.tic_id==lc1.tic_id,
            Lightcurve.aperture==lc1.aperture,
            Lightcurve.lightcurve_type==lc1.lightcurve_type
        ).one()

        assert arr_equal(check.cadences, lc1.cadences)
        assert arr_equal(check.bjd, lc1.bjd)
        assert arr_equal(check.values, new_val)
        assert arr_equal(check.errors, lc1.errors)
        assert arr_equal(check.x_centroids, lc1.x_centroids)
        assert arr_equal(check.y_centroids, lc1.y_centroids)
        assert arr_equal(check.quality_flags, lc1.quality_flags)

        check = db_conn.lightcurves.filter(
            Lightcurve.tic_id==lc2.tic_id,
            Lightcurve.aperture==lc2.aperture,
            Lightcurve.lightcurve_type==lc2.lightcurve_type
        ).one()

        assert arr_equal(check.cadences, lc2.cadences)
        assert arr_equal(check.bjd, lc2.bjd)
        assert arr_equal(check.values, lc2.values)
        assert arr_equal(check.errors, lc2.errors)
        assert arr_equal(check.x_centroids, lc2.x_centroids)
        assert arr_equal(check.y_centroids, lc2.y_centroids)
        assert arr_equal(check.quality_flags, lc2.quality_flags)

    except Exception:
        db_conn.session.rollback()
        raise
    finally:
        db_conn.session.rollback()
        for q in clear_all():
            db_conn.session.execute(q)
        db_conn.commit()


@given(st.data())
def test_best_apertures(db_conn, data):
    try:
        db_conn.session.begin_nested()
        apertures = data.draw(
                st.lists(
                    aperture_st(),
                    unique_by=(
                        lambda a: (a.star_r, a.inner_r, a.outer_r),
                        lambda a: a.name
                    ),
                    min_size=2
                )
        )

        db_conn.session.add_all(apertures)
        db_conn.commit()

        lc_type = data.draw(lightcurve_type_st())
        db_conn.session.add(lc_type)
        db_conn.commit()

        lightcurves = data.draw(
            st.lists(
                lightcurve_st(
                    aperture=st.sampled_from(apertures),
                    lightcurve_type=st.just(lc_type),
                ),
                unique_by=lambda lc: lc.tic_id,
                min_size=1
            )
        )
        db_conn.session.add_all(lightcurves)
        db_conn.commit()

        tics = {lc.tic_id for lc in lightcurves}
        aperture = sample([lc.aperture for lc in lightcurves], 1)[0]
        for tic in tics:
            db_conn.set_best_aperture(tic, aperture)
        db_conn.commit()

        best_apertures = db_conn.lightcurves_from_best_aperture(
            db_conn.lightcurves
        )

        assert all(lc.aperture == aperture for lc in best_apertures)
        db_conn.session.rollback()

    except Exception:
        db_conn.session.rollback()
        raise
    finally:
        db_conn.session.rollback()
        for q in clear_all():
            db_conn.session.execute(q)
        db_conn.commit()
