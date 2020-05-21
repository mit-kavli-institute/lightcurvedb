from h5py import File as H5File
from datetime import datetime
from sqlalchemy import Sequence
from lightcurvedb.models import Aperture, LightcurveType, Lightcurve
from lightcurvedb.util.iter import chunkify
import numpy as np
import numba as nb
import os
from .base import Ingestor

def quality_flag_extr(qflags):
    accept = np.ones(qflags.shape[0], dtype=np.int64)
    for i in range(qflags.shape[0]):
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

def bulk_ingest_lightcurves(db, lightcurves, chunksize=10000):
    id_seq = Sequence('qlpdataproducts_pk_table')
    for chunk in chunkify(lightcurves, chunksize):
        # We need to make a QLPDataproduct model for each one
        qlpdataproducts = []
        for lightcurve in chunk:
            now = datetime.now()
            qlpdp = {
                'id': db.session.execute(id_seq),
                'created_on': now,
                'product_type': LightcurveRevision.__tablename__
            }
            qlpdataproducts.append(qlpdata)
            lightcurve['id'] = qlpdp['id']
        db.session.bulk_insert_mappings(QLPDataProduct, qlpdataproducts)
        db.session.bulk_insert_mappings(Lightcurve, chunk)


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
            quality_flags = quality_flag_extr(compound_lc['QualityFlag'][()])
            for lc_type, has_error in H5_LC_TYPES.items():
                result = {
                    'lc_type': lc_type,
                    'aperture': aperture,
                    'tic': tic,
                }
                values = compound_lc[lc_type][()]

                if has_error:
                    errors = compound_lc['{}Error'.format(lc_type)][()]
                else:
                    errors = np.full_like(cadences, np.nan, dtype=np.double)

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
