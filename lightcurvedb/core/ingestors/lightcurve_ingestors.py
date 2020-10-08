import os
import re
import warnings

import numpy as np
import pandas as pd


with warnings.catch_warnings():
    warnings.simplefilter('ignore', category=FutureWarning)
    from h5py import File as H5File


THRESHOLD = 1 * 10**9 / 4  # bytes


path_components = re.compile(
    (
        r'orbit-(?P<orbit>[1-9][0-9]*)/'
        r'ffi/'
        r'cam(?P<camera>[1-4])/'
        r'ccd(?P<ccd>[1-4])/'
        r'LC/'
        r'(?P<tic>[1-9][0-9]*)'
        r'\.h5$'
    )
)


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


def h5_to_kwargs(filepath, **constants):
    with H5File(filepath, 'r') as h5in:
        lc = h5in['LightCurve']
        tic = int(os.path.basename(filepath).split('.')[0])
        cadences = lc['Cadence'][()].astype(int)
        bjd = lc['BJD'][()]
        apertures = lc['AperturePhotometry'].keys()

        for aperture in apertures:
            lc_by_ap = lc['AperturePhotometry'][aperture]
            x_centroids = lc_by_ap['X'][()]
            y_centroids = lc_by_ap['Y'][()]
            quality_flags = quality_flag_extr(
                lc_by_ap['QualityFlag'][()]
            ).astype(int)

            for lc_type, has_error_field in H5_LC_TYPES.items():
                if lc_type not in lc_by_ap:
                    continue
                values = lc_by_ap[lc_type][()]
                if has_error_field:
                    errors = lc_by_ap['{0}Error'.format(lc_type)][()]
                else:
                    errors = np.full_like(cadences, np.nan, dtype=np.double)

                yield dict(
                    tic_id=tic,
                    lightcurve_type_id=lc_type,
                    aperture_id=aperture,
                    cadences=cadences,
                    barycentric_julian_date=bjd,
                    values=values,
                    errors=errors,
                    x_centroids=x_centroids,
                    y_centroids=y_centroids,
                    quality_flags=quality_flags,
                    **constants
                )


def kwargs_to_df(*kwargs, **constants):
    dfs = []
    keys = ['cadences', 'barycentric_julian_date', 'values', 'errors',
            'x_centroids', 'y_centroids', 'quality_flags']

    for kwarg in kwargs:
        df = pd.DataFrame(
            data={
                k: kwarg[k] for k in keys
            }
        )
        df['lightcurve_id'] = kwarg['id']
        df = df.set_index(['lightcurve_id', 'cadences'])
        dfs.append(df)
    main = pd.concat(dfs)
    for k, constant in constants.items():
        main[k] = constant
    return main
