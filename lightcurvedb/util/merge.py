import numpy as np
import numba as nb
import pandas as pd
import sys

if sys.version_info.major >= 3:
    rangefunc = range
else:
    rangefunc = xrange

INDEX_COL = 'cadences'
INDEX_NTH = 0

EXPECTED_COLS = [
    'barycentric_julian_date',
    'values',
    'errors',
    'x_centroids',
    'y_centroids',
    'quality_flags',
]

#@nb.jit(nopython=True, cache=True)
def matrix_merge(*arrays):
    data = np.concatenate(arrays, axis=1)
    ref_row = data[0]
    path = np.argsort(ref_row)
    check = np.append(np.diff(ref_row[path]), 1)  # Always append last element

    return data[:,path[check > 0]]

