from h5py import File as H5File
from lightcurvedb.models import Aperture, LightcurveType, Lightcurve
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


class LightcurveH5Ingestor(Ingestor):

    EmissionModel = Lightcurve
    apertures = None
    lightcurve_types = ['KSPMagnitude', 'RawMagnitude']

    def __init__(self, *args, **kwargs):
        super(LightcurveH5Ingestor, self).__init__(*args, **kwargs)
        lc_type_q = self.context['db'].session.query(LightcurveType).filter(LightcurveType.name.in_(self.lightcurve_types))


        self.type_cache = {t.name: t for t in lc_type_q.all()}
        print(self.type_cache)


    def load_apertures(self, h5_lc):
        # Check if we've already loaded aperture contexts
        if self.apertures:
            return
        # No apertures have been defined
        aperture_names = h5_lc['AperturePhotometry'].keys()
        q = self.context['db'].apertures.filter(Aperture.name.in_(aperture_names))

        self.apertures = {aperture.name: aperture for aperture in q.all()}

    def parse(self, descriptor):
        with H5File(descriptor, 'r') as h5in:
            # Check
            h5_lc = h5in['LightCurve']
            self.load_apertures(h5_lc)
            filename = os.path.basename(descriptor.name).split('.')[0]
            tic_id = int(filename)

            # Load common attributes that are independent of aperture phot.
            cadences = h5_lc['Cadence'][()]
            bjd = h5_lc['BJD'][()]

            for aperture_name, aperture in self.apertures.items():
                compound_lc = h5_lc['AperturePhotometry'][aperture_name]
                x_centroids = compound_lc['X'][()]
                y_centroids = compound_lc['Y'][()]
                quality_flags = quality_flag_extr(compound_lc['QualityFlag'][()])
                # Two types of lightcurves, KSPMagnitude and RawMagnitude
                for type_name, lc_type in self.type_cache.items():
                    flux = compound_lc[type_name][()]
                    try:
                        flux_err = compound_lc['{}Error'.format(type_name)]
                    except KeyError:
                        flux_err = np.zeros(len(flux))
                    yield {
                        'tic_id': tic_id,
                        'aperture_id': aperture.id,
                        'lightcurve_type_id': lc_type.id,
                        'cadences': cadences,
                        'bjd': bjd,
                        'flux': flux,
                        'flux_err': flux_err,
                        'x_centroids': x_centroids,
                        'y_centroids': y_centroids,
                        'quality_flags': quality_flags
                    }
