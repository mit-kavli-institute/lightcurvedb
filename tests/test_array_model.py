from hypothesis import given, note, assume, settings, HealthCheck
from hypothesis.extra import numpy as np_st
from lightcurvedb import models
from lightcurvedb.util.merge import matrix_merge
from sqlalchemy import func, select
import numpy as np

from .fixtures import db_conn
from .factories import lightcurve as lightcurve_st

@settings(suppress_health_check=[HealthCheck.too_slow])
@given(lightcurve_st())
def test_instantiation(lightcurve):
    assert lightcurve is not None
    assert len(lightcurve) >= 0


@settings(suppress_health_check=[HealthCheck.too_slow])
@given(np_st.arrays(np.int32,(2, 100)), np_st.arrays(np.int32,(2, 100)))
def test_merging_sorts(arr1, arr2):
    result = matrix_merge(arr1, arr2)
    note(result)
    sort_ref = result[0]
    assert all(np.diff(sort_ref) >= 0)

@settings(suppress_health_check=[HealthCheck.too_slow])
@given(np_st.arrays(np.int32,(2, 100)), np_st.arrays(np.int32,(2, 100)))
def test_merging_unique(arr1, arr2):
    result = matrix_merge(arr1, arr2)
    note(result)
    sort_ref = result[0]
    check = set(sort_ref)
    assert len(sort_ref) == len(check)


@settings(suppress_health_check=[HealthCheck.too_slow])
@given(lightcurve_st())
def test_lightcurve_add(db_conn, lightcurve):
    try:
        db_conn.session.begin_nested()
        db_conn.session.add(lightcurve)
        db_conn.session.commit()

        q = select([func.count()]).select_from(models.Lightcurve)
        result = db_conn.session.execute(q).scalar()
        assert result == 1
        db_conn.session.rollback()
    except:
        note(lightcurve)
        db_conn.session.rollback()
        raise


@settings(deadline=None)
@given(np_st.arrays(np.int32,(2, 100)), np_st.arrays(np.int32,(2, 100)))
def test_merging_priority(arr1, arr2):
    # Prioritize later arrays
    result = matrix_merge(arr1, arr2)

    #data = np.concatenate((arr1, arr2), axis=1)
    #ref_row = data[0]
    #path = np.argsort(ref_row)
    #check = np.concatenate((np.diff(ref_row[path]), [1]))

    #result = data[:,path[check > 0]]

    check = set(result[0])
    ref1 = set(arr1[0])
    ref2 = set(arr2[0])

    #note(result)
    #note(check)
    #note(ref1)
    #note(ref2)

    assert ref2 <= check
    assert ref1 <= check

    #assert np.array_equal(what, result)
