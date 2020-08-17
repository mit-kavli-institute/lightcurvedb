from hypothesis import strategies as st, given
from lightcurvedb.util.iter import eq_partitions
from itertools import chain


@given(
    st.iterables(st.integers()),
    st.integers(min_value=1, max_value=100)
)
def test_partition_eq_splitting(iterable, n_partitions):
    """
    Test that we actually create the correct number of partitions
    """
    partitions = eq_partitions(iterable, n_partitions)

    assert len(partitions) == n_partitions


@given(
    st.iterables(st.integers()),
    st.integers(min_value=1, max_value=100)
)
def test_partition_elements(iterable, n_partitions):
    """
    Test that all the items are passed into the partitions and that no
    duplicates are created.
    """
    items = list(iterable)
    ref = set(items)
    partitions = eq_partitions(items, n_partitions)

    n_items = 0
    for item in chain.from_iterable(partitions):
        assert item in ref
        n_items += 1

    assert n_items == len(items)
