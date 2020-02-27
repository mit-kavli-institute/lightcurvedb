from hypothesis import strategies as st
from hypothesis import given
import pytest

from lightcurvedb.util.iter import chunkify, enumerate_chunkify, pop_chunkify


@given(st.integers(min_value=1), st.iterables(st.just(None)))
def test_chunkify(chunksize, iterable):
    chunks = chunkify(iterable, chunksize)
    assert all(len(chunk) <= chunksize for chunk in chunks)


@given(st.integers(max_value=0), st.iterables(st.just(None)))
def test_invalid_chunkify(chunksize, iterable):
    with pytest.raises(ValueError):
        chunks = list(chunkify(iterable, chunksize))


@given(st.integers(min_value=1), st.lists(st.just(None)))
def test_chunkify_w_list(chunksize, list_like):
    chunks = chunkify(list_like, chunksize)
    assert all(len(chunk) <= chunksize for chunk in chunks)


@given(st.integers(min_value=1), st.integers(), st.iterables(st.just(None)))
def test_enumerate_chunkify(chunksize, offset, iterable):
    chunks = enumerate_chunkify(iterable, chunksize, offset=offset)
    seen_indices = set()
    for chunk in chunks:
        assert len(chunk) <= chunksize
        for nth, _ in chunk:
            assert nth not in seen_indices
            seen_indices.add(nth)


@given(st.integers(min_value=1), st.integers(), st.lists(st.just(None)))
def test_enumerate_chunkify_w_list(chunksize, offset, iterable):
    chunks = enumerate_chunkify(iterable, chunksize, offset=offset)
    seen_indices = set()
    for chunk in chunks:
        assert len(chunk) <= chunksize
        for nth, _ in chunk:
            assert nth not in seen_indices
            assert nth >= offset
            seen_indices.add(nth)


@given(st.integers(min_value=1), st.lists(st.just(None)))
def test_pop_chunkify(chunksize, listlike):
    chunks = pop_chunkify(listlike, chunksize)
    assert all(len(chunk) <= chunksize for chunk in chunks)

    # Assert that chunkify has consumed the list
    assert len(listlike) == 0
