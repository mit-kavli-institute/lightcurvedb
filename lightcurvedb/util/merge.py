import numpy as np
import sys

if sys.version_info.major >= 3:
    rangefunc = range
else:
    rangefunc = xrange


def matrix_merge(arr1, arr2, **kwargs):
    data = np.concatenate((np.copy(arr1), np.copy(arr2)), axis=1)
    ref_row = data[0]
    path = np.argsort(ref_row)
    check = np.concatenate((np.diff(ref_row[path]), [1]))  # Always append last element

    return data[:,path[check > 0]]
