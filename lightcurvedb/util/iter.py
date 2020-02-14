import sys
PY_V = sys.version_info

if PY_V.major >= 3:
    from itertools import zip_longest as zip
else:
    from itertools import izip_longest as zip


def chunkify(chunksize, iterable, fillvalue=None):
    args = [iter(iterable)] * chunksize
    return zip(*args, fillvalue=fillvalue)
