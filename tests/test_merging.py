import numpy as np
from hypothesis import strategies as st, given, note
from hypothesis.extra import numpy as np_st

from lightcurvedb.util.merge import merge_arrays

DATA_TYPES = (
    np.int32,
    np.int64,
    np.float32,
    np.float64
)


@given(st.data())
def test_tautological_merge(data):
    length = data.draw(st.integers(min_value=1, max_value=255))
    ref_array = np.arange(length)
    data_array = np.arange(length)

    sorted_ref, merged_data = merge_arrays(ref_array, data=data_array)

    # Since everything was sorted. All returned arrays should be the
    # same
    assert np.array_equal(ref_array, sorted_ref)
    assert np.array_equal(data_array, merged_data['data'])

    index = np.arange(length)

    reversed = index[::-1]
    rev_data_array = index[::-1]

    sorted_ref, merged_data = merge_arrays(reversed, data=rev_data_array)

    # Since everything was reverse sorted. All returned arrays should
    # be backward
    assert np.array_equal(sorted_ref[::-1], reversed)
    assert np.array_equal(data_array[::-1], rev_data_array)


@given(st.data())
def test_non_duplicate_merge(data):
    length = data.draw(st.integers(min_value=1, max_value=10))

    # Use this length to make congruent np arrays...
    data_type = data.draw(st.sampled_from(DATA_TYPES))
    ref_array = data.draw(
        np_st.arrays(
            np.int32,
            length,
            unique=True
        )
    )
    data_array = data.draw(
        np_st.arrays(
            data_type,
            length
        )
    )

    path = np.argsort(ref_array)

    sorted_ref, merged_data = merge_arrays(ref_array, data=data_array)

    # Assert sorted
    assert all(np.diff(sorted_ref) >= 0)

    # Assert that the data returned is in a different order
    np.testing.assert_equal(
        data_array[path],
        merged_data['data']
    )


@given(st.data())
def test_duplicate_merge(data):
    length = data.draw(st.integers(min_value=1, max_value=10))

    index = np.concatenate((
        np.arange(length),
        np.arange(length)
    ))
    values = np.concatenate((
        np.full(length, 1),
        np.full(length, 2)
    ))

    sorted_ref, merged_data = merge_arrays(index, data=values)

    assert len(sorted_ref) == length
    assert len(merged_data['data']) == length

    note(sorted_ref)
    note(merged_data['data'])

    np.testing.assert_equal(
        sorted_ref, np.arange(length)
    )

    assert all(merged_data['data'] == 2)

