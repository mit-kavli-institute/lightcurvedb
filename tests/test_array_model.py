from hypothesis import given, note, assume
from hypothesis.extra import numpy as np_st
from lightcurvedb import models
from lightcurvedb.util.merge import matrix_merge
import numpy as np

from .fixtures import db_conn
from .factories import array_lightcurve as lightcurve_st

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

@given(np_st.arrays(np.int32,(2, 100)), np_st.arrays(np.int32,(2, 100)))
def test_merging_priority(arr1, arr2):
    # Prioritize later arrays
    result = matrix_merge(arr1, arr2)
    check = set(result[0])
    ref1 = set(arr1[0])
    ref2 = set(arr2[0])

    note(result)
    note(ref1)
    note(ref2)

    assert ref2 <= check
    assert ref1 <= check
