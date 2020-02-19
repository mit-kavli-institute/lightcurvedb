import sys
PY_V = sys.version_info

if PY_V.major >= 3:
    from itertools import zip_longest as zip
else:
    from itertools import izip_longest as zip


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
    for _ in range(starting_len):
        next_item = listlike.pop()
        chunk.append(next_item)
        if len(chunk) >= chunksize:
            yield chunk
            chunk = []

    # Cleanup
    if len(chunk) > 0:
        yield chunk
