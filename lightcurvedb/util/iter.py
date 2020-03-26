from __future__ import division, print_function
import sys

PY_V = sys.version_info

if PY_V.major >= 3:
    from itertools import zip_longest as zip
else:
    from itertools import izip_longest as zip
import itertools


def chunkify(iterable, chunksize, fillvalue=None):
    chunk = []
    if chunksize < 1:
        raise ValueError(
            'Chunkify command cannot have a chunksize < 1'
        )
    for item in iterable:
        chunk.append(item)
        if len(chunk) >= chunksize:
            yield chunk
            chunk = []

    # Cleanup
    if len(chunk) > 0:
        yield chunk


def enumerate_chunkify(iterable, chunksize, offset=0, fillvalue=None):
    """
    Chunkify's an iterable and provides the nth iteration to each chunk
    element. This can be offset by the :start_id: value.
    """
    chunk = []
    if chunksize < 1:
        raise ValueError(
            'Chunkify command cannot have a chunksize < 1'
        )
    for ith, item in enumerate(iterable):
        chunk.append((ith+offset, item))
        if len(chunk) >= chunksize:
            yield chunk
            chunk = []

    # Cleanup
    if len(chunk) > 0:
        yield chunk

def pop_chunkify(listlike, chunksize):
    if chunksize < 1:
        raise ValueError(
            'Chunkify command cannot have a chunksize < 1'
        )
    starting_len = len(listlike)
    chunk = []
    try:
        for _ in range(starting_len):
            next_item = listlike.pop()
            chunk.append(next_item)
            if len(chunk) >= chunksize:
                yield chunk
                chunk = []
    except TypeError:
        # Attempt to recover in event of dictlike
        keys = set(listlike.keys())  # We need to copy to break reference
        for key in keys:
            next_item = listlike.pop(key)
            chunk.append((key, next_item))
            if len(chunk) >= chunksize:
                yield chunk
                chunk = []
    # Cleanup
    if len(chunk) > 0:
        yield chunk


def split_every(nth, iterable):
    i = iter(iterable)
    splice = list(itertools.islice(i, nth))
    while splice:
        yield splice
        splice = list(itertools.islice(i, nth))


def partition(listlike, n):
    if n < 1:
        raise ValueError(
            'Cannot create a partition of size < 1'
        )

    max_partition_length = len(listlike) // n
    return split_every(max_partition_length, listlike)


def partition_by(listlike, n, key=lambda x: x):
    if n < 1:
        raise ValueError(
            'Cannot create partitions of size < 1'
        )
    groups = [(k, list(g)) for k, g in itertools.groupby(listlike, key=key)]
    return partition(groups, n)
