import numpy as np


def get_index(array, v):
    """
    Get the index of v in the sorted unique array.
    If the v cannot be found the an IndexError is
    returned.
    """
    if v < array[0] or v > array[-1]:
        raise KeyError(
            '{} resides outside of {}'.format(v, array)
        )
    cur_index = len(array) // 2

    while cur_index > 0 and cur_index < len(array):
        pass


def cadence_path(orig_cadence, cadence_search):
    """
    Grab indices what each cadence appears in. If no cadence
    exists in the original cadence then it will not have a
    representation in the index-array.

    Arguements
    ----------
    orig_cadence: np.ndarray
        A sorted and 1-D array of unique values.
    cadence_search: np.ndarray
        A flat array of values to return indices for.
    """
    pass
