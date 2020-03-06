import numpy as np
import sys

if sys.version_info.major >= 3:
    rangefunc = range
else:
    rangefunc = xrange


def matrix_merge(*matrices, **kwargs):
    sort_row_index = kwargs.pop('sort_row_index', 0)
    data = np.concatenate(matrices, axis=1)
    reference_row = data[sort_row_index]

    sort_path = np.argsort(reference_row)
    shortened_path = []

    for i in rangefunc(len(sort_path)):
        try:
            cur_index = sort_path[i]
            next_index = sort_path[i+1]
        except IndexError:
            shortened_path.append(cur_index)
            break

        cur_data = reference_row[cur_index]
        next_data = reference_row[next_index]
        if cur_data == next_data:
            # Skip duplicate value
            continue
        shortened_path.append(cur_index)

    path = np.array(shortened_path)
    return data[:,path]
