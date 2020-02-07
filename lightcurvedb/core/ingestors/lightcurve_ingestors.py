from h5py import File as H5File
from lightcurvedb.models import Aperture, LightcurveType, Lightcurve
import numpy as np
import numba as nb
from .base import Ingestor


@nb.jit(nogil=True, cache=True, parallel=True)
def quality_flag_extr(qflags):
    accept = np.ones(qflags.shape)
    np.puts(accept, np.where(qflags != b'G'), 0)
    return accept


class LightcurveH5Ingestor(Ingestor):

    EmissionModel = Lightcurve
    apertures = None
    lightcurve_types = ['KSPMagnitude', 'RawMagnitude']

    def __init__(self, *args, **kwargs):
        super(LightCurveH5Ingestor, self).__init__(*args, **kwargs)
        lc_type_q = self.context['db'].session.query(LightcurveType).filter(LightcurveType.name._in == (self.lightcurve_types))


        self.type_cache = {t.name: t for t in lc_type_q.all()}


    def load_apertures(self, h5_lc):
        # Check if we've already loaded aperture contexts
        if self.apertures:
            return
        # No apertures have been defined
        aperture_names = h5in['AperturePhotometry']['Apertures'].keys()
        q = self.context['db'].apertures.filter(Aperture.name._in(aperture_names))

        self.apertures = {aperture.name: aperture for aperture in q.all()}

    def parse(self, path):
        with H5File(path, 'r') as h5in:
            # Check
            h5_lc = h5in['LightCurve']
            self.load_apertures(h5_lc)

            tic_id = int(h5in.filename.split('.')[0])

            # Load common attributes that are independent of aperture phot.
            cadences = h5_lc['Cadence'].value
            bjd = h5_lc['BJD'].value

            for aperture_name, aperture in self.apertures.items():
                compound_lc = h5_lc['AperturePhotometry'][aperture_name]
                x_centroids = compound_lc['X'].value
                y_centroids = compound_lc['Y'].value
                quality_flags = qualtity_flag_extr(compound_lc['QualityFlag'])
                # Two types of lightcurves, KSPMagnitude and RawMagnitude
                for type_name, lc_type in self.type_cache.items():
                    flux = compound_lc[type_name].value
                    try:
                        flux_err = compound_lc['{}Error'.format(type_name)]
                    except KeyError:
                        flux_err = np.zeros(len(flux))
                    yield {
                        'tic_id': tic_id,
                        'aperture_id': aperture_id,
                        'lightcurve_type_id': lightcurve_type.id,
                        'cadences': cadences,
                        'bjd': bjd,
                        'flux': flux,
                        'flux_err': flux_err,
                        'x_centroids': x_centroids,
                        'y_centroids': y_centroids,
                        'quality_flags': quality_flags
                    }
