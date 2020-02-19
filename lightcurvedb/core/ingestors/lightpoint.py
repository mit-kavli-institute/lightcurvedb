from .lightcurve_ingestors import h5_to_matrices

def get_raw_h5(filepath):
    return list(h5_to_matrices(filepath))
