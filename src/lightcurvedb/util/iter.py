from collections.abc import Generator, Iterable
from typing import TypeVar

T = TypeVar("T")


def chunkify(
    iterable: Iterable[T], chunksize: int, fillvalue: T | None = None
) -> Generator[list[T], None, None]:
    """
    Chunkify an iterable into equal sized partitions.

    The last chunk yielded might contain leftovers and have a length less
    than specified. If a ``fillvalue`` is specified then this value will be
    padded in the last chunk until it meets the chunksize requirement.

    Parameters
    ----------
    iterable
        Some iterable to chunkify into partitions.
    chunksize
        The size of the returned partitions. Must be greater than 0.
    fillvalue
        If the last partition has length < chunksize, right pad the
        partition with the ``fillvalue`` until the wanted partition size
        is reached.

    Yields
    ------
    list[T]
        A partitioned list of length <= chunksize.

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
        if fillvalue is not None:
            while len(chunk) < chunksize:
                chunk.append(fillvalue)
        yield chunk


def eq_partitions(iterable: Iterable[T], n: int) -> tuple[list[T], ...]:
    """
    Create ``n`` partitions and distribute the iterable as equally as possible.

    Parameters
    ----------
    iterable
        Some iterable to partition into ``n`` lists.
    n
        The number of partitions to create. Cannot be less than 1.
        If this number is greater than the number of items within the
        given iterable, then it is guaranteed that some lists will be
        empty.

    Returns
    -------
    tuple[list[T], ...]
        A tuple of lists. The tuple is length of ``n``. The
        lists contained within will be variant in length.

    Raises
    ------
    ValueError
        Raised if ``n`` is less than 1.
    """
    if n < 1:
        raise ValueError("Number of partitions must be at least 1")

    partitions = tuple([] for _ in range(n))

    for i, item in enumerate(iterable):
        partition = partitions[i % n]
        partition.append(item)

    return partitions
