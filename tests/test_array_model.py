from hypothesis import given, note, assume, settings
from hypothesis.extra import numpy as np_st
from lightcurvedb import models
from lightcurvedb.util.merge import matrix_merge
import numpy as np

from .fixtures import db_conn
from .factories import lightcurve as lightcurve_st

@given(lightcurve_st())
def test_instantiation(lightcurve):
    assert lightcurve is not None
    assert len(lightcurve) >= 0


@given(np_st.arrays(np.int32,(2, 100)), np_st.arrays(np.int32,(2, 100)))
def test_merging_sorts(arr1, arr2):
    result = matrix_merge(arr1, arr2)
    note(result)
    sort_ref = result[0]
    assert all(np.diff(sort_ref) >= 0)

@given(np_st.arrays(np.int32,(2, 100)), np_st.arrays(np.int32,(2, 100)))
def test_merging_unique(arr1, arr2):
    result = matrix_merge(arr1, arr2)
    note(result)
    sort_ref = result[0]
    check = set(sort_ref)
    assert len(sort_ref) == len(check)

#@settings(deadline=None)
#@given(np_st.arrays(np.int32,(2, 100)), np_st.arrays(np.int32,(2, 100)))
#def test_merging_priority(arr1, arr2):
#    # Prioritize later arrays
#    arr1copy = np.copy(arr1)
#    arr2copy = np.copy(arr2)
#    result = matrix_merge(arr1copy, arr2copy)
#
#    #data = np.concatenate((arr1, arr2), axis=1)
#    #ref_row = data[0]
#    #path = np.argsort(ref_row)
#    #check = np.concatenate((np.diff(ref_row[path]), [1]))
#
#    #result = data[:,path[check > 0]]
#
#    check = set(result[0])
#    ref1 = set(arr1[0])
#    ref2 = set(arr2[0])
#
#    #note(result)
#    #note(check)
#    #note(ref1)
#    #note(ref2)
#
#    assert ref2 <= check
#    assert ref1 <= check
#
#    #assert np.array_equal(what, result)
