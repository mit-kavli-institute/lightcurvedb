from h5py import File as H5File
from lightcurvedb.models import Aperture, LightcurveType, Lightcurve, Lightpoint
import numpy as np
import numba as nb
import os
from .base import Ingestor

QFLAG_IN_TYPE =  nb.typeof(np.array([b'G']))
QFLAG_OUT_TYPE = nb.typeof(np.array([123]))
QFLAG_SIG = QFLAG_OUT_TYPE(QFLAG_IN_TYPE)


@nb.njit(QFLAG_SIG, cache=True, parallel=True)
def quality_flag_extr(qflags):
    accept = np.ones(qflags.shape[0], dtype=np.int64)
    for i in nb.prange(qflags.shape[0]):
        if qflags[i] == b'G':
            accept[i] = 1
        else:
            accept[i] = 0
    return accept

# Def: KEY -> Has error field
H5_LC_TYPES = {
    'KSPMagnitude': False,
    'RawMagnitude': True
}

def h5_to_matrices(filepath):
    with H5File(filepath, 'r') as h5in:
        # Iterate and yield extracted h5 interior data
        lc = h5in['LightCurve']
        tic = int(os.path.basename(filepath).split('.')[0])
        cadences = lc['Cadence'][()]
        bjd = lc['BJD'][()]

        apertures = lc['AperturePhotometry'].keys()
        for aperture in apertures:
            compound_lc = lc['AperturePhotometry'][aperture]
            x_centroids = compound_lc['X'][()]
            y_centroids = compound_lc['Y'][()]
            quality_flags_extr(compound_lc['QualityFlag'][()])
            for lc_type, has_error in H5_LC_TYPES.items():
                result = {
                    'lc_type': lc_type,
                    'aperture': aperture,
                    'tic': tic,
                }
                values = compound_lc[lc_type][()]

                if has_error:
                    errors = compound_lc['{}Error'.format(lc_type)][()]

                result['data'] = np.array([
                    cadences,
                    bjd,
                    values,
                    errors,
                    x_centroids,
                    y_centroids,
                    quality_flags
                ])

                yield result
