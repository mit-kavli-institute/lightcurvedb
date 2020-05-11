from hypothesis import given, note, assume, settings, HealthCheck
from hypothesis import strategies as st
from .factories import lightcurve as lightcurve_st
from .fixtures import db_conn, clear_all
from lightcurvedb.core.base_model import QLPModel
from lightcurvedb.managers.lightcurve_query import LightcurveManager
from lightcurvedb.models import Lightcurve, LightcurveType
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


@given(lightcurve_st(with_id=st.integers(min_value=1)))
def test_insert_manager_q(db_conn, lightcurve):
    try:
        db_conn.session.begin_nested()
        db_conn.session.add(lightcurve.lightcurve_type)
        db_conn.session.add(lightcurve.aperture)
        db_conn.commit()
        note(lightcurve.cadences)
        manager = LightcurveManager([], internal_session=db_conn)
        manager.add(
            lightcurve.tic_id,
            lightcurve.aperture,
            lightcurve.lightcurve_type,
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