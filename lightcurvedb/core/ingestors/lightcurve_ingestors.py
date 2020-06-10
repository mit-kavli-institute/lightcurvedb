from h5py import File as H5File
from datetime import datetime
from sqlalchemy import Sequence
from lightcurvedb.models import Aperture, LightcurveType, Lightcurve
from lightcurvedb.util.iter import chunkify
import numpy as np
import pandas as pd
import os
import re
import sqlite3
import itertools
from copy import deepcopy
from .base import Ingestor


path_components = re.compile(r'orbit-(?P<orbit>[1-9][0-9]*)/ffi/cam(?P<camera>[1-4])/ccd(?P<ccd>[1-4])/LC/(?P<tic>[1-9][0-9]*)\.h5$')


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

def h5_to_observation(filepath):
    context = path_components.search(filepath).groupdict()
    mapped = deepcopy(context)
    mapped['tic_id'] = mapped['tic']
    del mapped['tic']
    return mapped

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


def h5_to_kwargs(filepath):
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
            quality_flags = quality_flag_extr(lc_by_ap['QualityFlag'][()]).astype(int)

            for lc_type, has_error_field in H5_LC_TYPES.items():
                values = lc_by_ap[lc_type][()]
                if has_error_field:
                    errors = lc_by_ap['{}Error'.format(lc_type)][()]
                else:
                    errors = np.full_like(cadences, np.nan, dtype=np.double)

                yield {
                    'tic_id': tic,
                    'lightcurve_type_id': lc_type,
                    'aperture_id': aperture,
                    'cadences': cadences,
                    'barycentric_julian_date': bjd,
                    'values': values,
                    'errors': errors,
                    'x_centroids': x_centroids,
                    'y_centroids': y_centroids,
                    'quality_flags': quality_flags
                }


def lc_dict_to_df(dictionary):
    return pd.DataFrame(
         {
            'barycentric_julian_date': dictionary['barycentric_julian_date'],
            'values': dictionary['values'],
            'errors': dictionary['errors'],
            'x_centroids': dictionary['x_centroids'],
            'y_centroids': dictionary['y_centroids'],
            'quality_flags': dictionary['quality_flags']
        },
        index=dictionary['cadences']
    )


def merge_h5(*h5_files):
    pid = os.getpid()
    kwargs = []

    for h5 in h5_files:
        for kwarg in h5_to_kwargs(h5):
            kwargs.append(kwarg)
    tic_id = kwargs[0]['tic_id']
    apertures = {kwarg['aperture_id'] for kwarg in kwargs}
    types = {kwarg['lightcurve_type_id'] for kwarg in kwargs}

    for aperture, lc_type in itertools.product(apertures, types):

        parallel_lcs = list(
            filter(
                lambda k: k['aperture_id'] == aperture and k['lightcurve_type_id'] == lc_type,
                kwargs
            )
        )
        ref = deepcopy(parallel_lcs[0])

        cadence_lengths = ' '.join(map(lambda p: str(len(p['cadences'])), parallel_lcs))
        values_lengths = ' '.join(map(lambda p: str(len(p['values'])), parallel_lcs))

        dfs = [lc_dict_to_df(kwarg) for kwarg in parallel_lcs]

        merged = pd.concat(dfs)
        merged = merged[~merged.index.duplicated(keep='last')]
        merged.sort_index(inplace=True)

        yield {
            'tic_id': tic_id,
            'aperture_id': aperture,
            'lightcurve_type_id': lc_type,
            'cadences': np.array(merged.index),
            'barycentric_julian_date': np.array(merged.barycentric_julian_date),
            'values': np.array(merged['values']),
            'errors': np.array(merged.errors),
            'x_centroids': np.array(merged.x_centroids),
            'y_centroids': np.array(merged.y_centroids),
            'quality_flags': np.array(merged.quality_flags)
        }


def parallel_h5_merge(file_group):
    merged = list(merge_h5(*file_group))
    observations = [h5_to_observation(f) for f in file_group]

    return observations, merged
    

class LightpointCache(object):
    def __init__(self, uri=':memory:', create_index=True):
        self.uri = uri
        self._db = sqlite3.connect(uri, uri=True)

        index_cols = ['lightcurve_id', 'cadence']
        self._db.execute(
            'CREATE TABLE IF NOT EXISTS '
            'lightpoints ( '
            'id INTEGER PRIMARY KEY AUTOINCREMENT, '
            'lightcurve_id INTEGER NOT NULL, '
            'cadence INTEGER NOT NULL, '
            'barycentric_julian_date REAL NOT NULL, '
            'value REAL, '
            'error REAL, '
            'x_centroid REAL, '
            'y_centroid REAL, '
            'quality_flag INTEGER NOT NULL) '
        )
        if create_index:
            for col in index_cols:
                self._db.execute(
                    'CREATE INDEX idx_{} ON lightpoints ({})'.format(
                        col, col
                    )
                )

    def __del__(self):
        self._db.close()
        if os.path.exists(self.uri):
            os.remove(self.uri)

    def ingest_lc_df(self, dataframe, temp_lc_id, rename=True):
        if rename:
            aliased = dataframe.rename(
                columns={
                    'cadences': 'cadence',
                    'values': 'value',
                    'errors': 'error',
                    'x_centroids': 'x_centroid',
                    'y_centroids': 'y_centroid',
                    'quality_flags': 'quality_flag'
                }
            )
        else:
            aliased = dataframe
        aliased['cadence'] = aliased.index
        aliased = aliased.assign(lightcurve_id=lambda x: temp_lc_id)
        aliased.to_sql(
            'lightpoints',
            con=self._db,
            if_exists='append',
            index=False
        )

    def ingest_lc(self, lightcurve):
        df = lightcurve.to_df
        self.ingest_lc_df(df, lightcurve.id, rename=True)

    def ingest_dict(self, dictionary, id):
        df = lc_dict_to_df(dictionary)
        self.ingest_lc_df(df, id, rename=True)

    def get_lc(self, _id):
        cols = [
            'cadence',
            'barycentric_julian_date',
            'value',
            'error',
            'x_centroid',
            'y_centroid',
            'quality_flag'
        ]
        col_clause = ', '.join(cols)
        command = (
            f'SELECT {col_clause} FROM lightpoints '
            f'WHERE lightcurve_id = {_id} ORDER BY cadence'
        )
        result = pd.read_sql(
            command,
            self._db,
            index_col='cadence'
        )
        result = result[~result.index.duplicated(keep='last')]
        return result

    def yield_insert_kwargs(self, ids, id_col='_id'):
        for id in ids:
            data = self.get_lc(id)
            yield {
                id_col: id,
                'cadences': data.index,
                'barycentric_julian_date': data['barycentric_julian_date'],
                'values': data['values'],
                'errors': data['errors'],
                'x_centroids': data['x_centroids'],
                'y_centroids': data['y_centroids'],
                'quality_flags': data['quality_flags']
            }

    def get_lightcurve_ids(self):
        command = (
            'SELECT DISTINCT lightcurve_id FROM lightpoints'
        )
        return {result[0] for result in self._db.execute(command).fetchall()}


class TempLightcurveIDMapper(object):
    def __init__(self, uri=':memory:'):
        self._db = sqlite3.connect(uri, uri=True)

        self._db.execute(
            'CREATE TABLE IF NOT EXISTS '
            'temp_lightcurve_ids ( '
            'id INTEGER PRIMARY KEY, '
            'tic_id INTEGER NOT NULL, '
            'aperture_id TEXT NOT NULL, '
            'lightcurve_type_id TEXT NOT NULL )'
        )

        index_cols = ['tic_id', 'aperture_id', 'lightcurve_type_id']
        for col in index_cols:
            self._db.execute(
                'CREATE INDEX idx_{} ON temp_lightcurve_ids ({})'.format(
                    col, col
                )
            )
        self._db.execute(
            'CREATE INDEX idx_main_lookup on temp_lightcurve_ids '
            '(tic_id, aperture_id, lightcurve_type_id)'
        )

    def set_id(self, id, tic_id, aperture_id, lightcurve_type_id):
        command = (
            'INSERT INTO temp_lightcurve_ids'
            '(id, tic_id, aperture_id, lightcurve_type_id) VALUES '
            f'({id}, {tic_id}, "{aperture_id}", "{lightcurve_type_id}")'
        )
        try:
            self._db.execute(command)
        except sqlite3.OperationalError:
            print(command)
            raise

    def get_id(self, tic_id, aperture_id, lightcurve_type_id):
        result = self._db.execute(
            'SELECT id FROM temp_lightcurve_ids WHERE '
            f'tic_id = {tic_id} AND '
            f'aperture_id = "{aperture_id}" AND '
            f'lightcurve_type_id = "{lightcurve_type_id}"'
        ).fetchone()

        if result:
            return result[0]
        return None

    def get_id_by_dict(self, kwargs):
        return self.get_id(
            kwargs['tic_id'],
            kwargs['aperture_id'],
            kwargs['lightcurve_type_id']
        )

    def get_values(self, id):
        return self._db.execute(
            'SELECT tic_id, aperture_id, lightcurve_type_id FROM '
            'temp_lightcurve_ids WHERE id = {}'.format(id)
        ).fetchone()

    def get_new_values(self):
        return list(
            self._db.execute(
                'SELECT id, tic_id, aperture_id, lightcurve_type_id  FROM temp_lightcurve_ids WHERE id < 0'
            ).fetchall()
        )

    def get_defined_values(self):
        return list(
            self._db.execute(
                'SELECT id, tic_id, aperture_id, lightcurve_type_id  FROM temp_lightcurve_ids WHERE id > 0'
            ).fetchall()
        )
