from hypothesis import given, note, assume, settings, HealthCheck
from hypothesis import strategies as st
from .factories import lightcurve as lightcurve_st
from lightcurvedb.managers.lightcurve_query import LightcurveManager


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
    for type_ in types:
        filtered = set(filter(lambda lc: lc.type.name == type_, lightcurves))
        result = manager[type_]

        if len(result) == 1:
            assert len(filtered) == 1
        else:
            assert filtered == set(result)


@settings(deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(st.lists(lightcurve_st(), min_size=3, unique_by=lambda l: l.aperture))
def test_grouping_by_aperture(lightcurves):

    for i, lc in enumerate(lightcurves):
        lc.id = i

    apertures = {lc.aperture.name for lc in lightcurves}

    manager = LightcurveManager(lightcurves)
    assert len(manager) == len(lightcurves)
    note(apertures)
    note(manager.searchables)
    for aperture in apertures:
        filtered = set(filter(lambda lc: lc.aperture.name == aperture, lightcurves))
        result = manager[aperture]

        if len(result) == 1:
            assert len(filtered) == 1
        else:
            assert filtered == set(result)

