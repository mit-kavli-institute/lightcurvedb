from hypothesis import given, note, assume, settings, HealthCheck
from hypothesis import strategies as st
from .factories import lightcurve as lightcurve_st
from .fixtures import db_conn
from lightcurvedb.managers.lightcurve_query import LightcurveManager, LightcurveDaemon
from lightcurvedb.models import Lightcurve


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


# @settings(
#     suppress_health_check=[
#         HealthCheck.too_slow,
#         HealthCheck.filter_too_much
#     ])
# @given(st.lists(lightcurve_st()))
# def test_async_retrieve_daemon(db_conn, lightcurves):
#     manager = LightcurveDaemon(db_conn, max_queue=5)
#     db_conn.session.begin_nested()
#     try:
#         db_conn.session.add_all(lightcurves)
#         db_conn.commit()
#         ids = {lc.id for lc in lightcurves}
# 
#         q = Lightcurve.__table__.select(Lightcurve.id)\
#             .where(Lightcurve.id.in_(ids))
#         manager.open()
#         job_ref = manager.push(q)
#         result = manager.get(job_ref)
#         manager.close()
# 
#         # Assert that we don't do anything silly with the job itself,
#         # that should remain untouched
#         assert ids == db_conn.session.execute(q)
# 
#     except Exception:
#         db_conn.session.rollback()
#         raise
