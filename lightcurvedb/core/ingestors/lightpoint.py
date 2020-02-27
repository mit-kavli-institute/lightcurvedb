from .lightcurve_ingestors import h5_to_matrices
import bisect


def get_raw_h5(filepath):
    return list(h5_to_matrices(filepath))


def find_new_lightpoints(datablock, existing_lightcurve):

    # Assumes lightpoints are in ascending order with respect to
    # cadence and bjd
    lightpoints = existing_lightcurve.lightpoints

