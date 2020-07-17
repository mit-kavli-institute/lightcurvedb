import numpy as np
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


def merge_arrays(ref_array, **arrays):
    """
    Using the ref_array, sort the given arrays.:qa
    """
    path = np.argsort(ref_array, kind='stable')
    check = np.append(np.diff(ref_array[path]), 1)

    result = dict()
    for arr_name, arr in arrays.items():
        result[arr_name] = arr[path[check > 0]]

    return ref_array[path[check > 0]], result

