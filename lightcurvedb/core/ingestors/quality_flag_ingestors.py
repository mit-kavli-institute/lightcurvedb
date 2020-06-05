import sqlite3
import os
from itertools import product

def iter_qflag(path):
    for line in open(path, 'rt').readlines():
        raw_cadence, raw_flag = line.strip().split(' ')
        cadence = int(float(raw_cadence))
        flag = int(float(raw_flag))
        yield cadence, flag


class QualityFlagReference(object):
    def __init__(self, uri='file::memory:?cache=shared'):
        self._db = sqlite3.connect(uri, uri=True)

        self._db.execute(
            'CREATE TABLE IF NOT EXISTS '
            'quality_flags ( '
            'id INTEGER PRIMARY KEY AUTOINCREMENT, '
            'camera INTEGER NOT NULL, '
            'ccd INTEGER NOT NULL, '
            'cadence INTEGER NOT NULL, '
            'quality_flag INTEGER NOT NULL '
            ' )'
        )

    def ingest(self, orbit, data_path='/pdo/qlp-data/'):
        self._db.executemany(
            'INSERT INTO quality_flags(camera, ccd, cadence, quality_flag) '
            'values '
            '(?, ?, ?, ?)',
            self.yield_row(orbit, data_path)
        )

    def yield_row(self, orbit, data_path):
        for camera, ccd in product([1,2,3,4], [1,2,3,4]):
            path = os.path.join(
                data_path,
                'orbit-{}'.format(orbit),
                'ffi', 'run',
                'cam{}ccd{}_qflag.txt'.format(camera, ccd)
            )
            for cadence, flag in iter_qflag(path):
                yield camera, ccd, cadence, flag

    def get_flags(self, camera, ccd, cadences):
        cadence_string = ','.join(map(str, cadences))
        cur = self._db.execute(
            'SELECT quality_flag FROM quality_flags ' + \
            'WHERE camera = {} AND ccd = {} AND cadence IN ({})'.format(
                camera, ccd, cadence_string
            ) + \
            'ORDER BY cadence'
        )
        return list(ret[0] for ret in cur)
