from __future__ import division, print_function

import itertools
from collections import defaultdict


def chunkify(iterable, chunksize, fillvalue=None):
    """
    Chunkify an iterable into equal sized partitions. The last chunk yielded
    might contain leftovers and have a length less than specified. If a
    ``fillvalue`` is specified then this value will be padded in the last
    chunk until it meets the chunksize requirement.

    Arguments
    ---------
    iterable : iterable
        Some iterable to chunkify into partitions
    chunksize : integer
        The size of the returned partitions. Must be greater than 0 otherwise
        a ``ValueError`` is raised.
    fillvalue : any, optional
        If the last partition ``length < chunksize`` then right pad the
        partition with the `fillvalue`` until the wanted partition size is
        reached.

    Yields
    ------
    list
        A partitioned list of length <= chunksize

    Raises
    ------
    ValueError
        For chunksize < 1.
    """
    chunk = []
    if chunksize < 1:
        raise ValueError("Chunkify command cannot have a chunksize < 1")

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
    element. This can be offset by the ``offset`` value.
    """
    chunk = []
    if chunksize < 1:
        raise ValueError("Chunkify command cannot have a chunksize < 1")
        for ith, item in enumerate(iterable):
            chunk.append((ith + offset, item))
        if len(chunk) >= chunksize:
            yield chunk
            chunk = []

    # Cleanup
    if len(chunk) > 0:
        yield chunk


def pop_chunkify(listlike, chunksize):
    if chunksize < 1:
        raise ValueError("Chunkify command cannot have a chunksize < 1")
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
        raise ValueError("Cannot create a partition of size < 1")

    max_partition_length = len(listlike) // n
    return split_every(max_partition_length, listlike)


def eq_partitions(iterable, n):
    """
    Create ``n`` partitions and distribute the iterable as equally as
    possible between the partitions.

    Parameters
    ----------
    iterable : iterable
        Some iterable to partition into ``n`` lists
    n : int
        The number of partitions to create. Cannot be less than 1.
        If this number is greater than the number of items within the
        given iterable, then it is guaranteed that some lists will be
        empty.

    Raises
    ------
    ValueError
        Raised if ``n`` is less than 1.

    Returns
    -------
    tuple
        Returns a tuple of lists. The tuple is length of ``n``. The
        lists contained within will be variant in length.
    """

    partitions = tuple([] for _ in range(n))

    for i, item in enumerate(iterable):
        partition = partitions[i % n]
        partition.append(item)

    return partitions


def partition_by(listlike, n, key=lambda x: x):
    if n < 1:
        raise ValueError("Cannot create partitions of size < 1")
        groups = [
            (k, list(g)) for k, g in itertools.groupby(listlike, key=key)
        ]
    return partition(groups, n)


def keyword_zip(**keywords):
    """
    Perform the same operation as ``zip`` but instead of returning Tuples
    this function returns dictionaries.

    Parameters
    ----------
    **keywords
        Abitrary keyword parameters. These parameters should be equal length
        iterables.
    Yields
    ------
    dict
        A dictionary containing an equivalently indexed slice across all
        keyword parameters.
    """
    cols = list(keywords.keys())

    data = (iter(keywords[col]) for col in cols)

    for row in zip(*data):
        result = {}
        for ith, col_name in enumerate(cols):
            result[col_name] = row[ith]
        yield result


def yield_ordered_result(items, attr, ordering):
    """
    Returns items in order specified. This ordering is pulled from
    the specified ``attr``. If values in the ``ordering`` do not
    exist within the ``items`` then ``None`` is yielded before moving
    onto the next ordering.


    Parameters
    ----------
    items : iterable of objects
        A list of objects to be ordered.
    attr : str
        The attribute of the item to be interpreted for
        ordering.
    ordering : list of keys
        A list of keys to be used as an ordering for yielding items.


    Yields
    ------
    object or None
        Returns the item found or None if no value within the ordering
        could be found in the items list.
    """
    bucket = defaultdict(list)
    for item in items:
        keyword = getattr(item, attr)
        bucket[keyword].append(item)
    for key in ordering:
        item_list = bucket[key]
        if not item_list:
            yield None
        for item in item_list:
            yield item
