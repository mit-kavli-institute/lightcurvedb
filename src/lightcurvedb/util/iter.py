from __future__ import division, print_function


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
