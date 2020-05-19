import numpy as np
from functools import reduce

def arr_equal(*arrays):
    masks = [np.isnan(a) for a in arrays]
    mask = ~reduce(lambda l, r: l | r, masks)
    return reduce(lambda l, r: np.allclose(l[mask], r[mask]), arrays)