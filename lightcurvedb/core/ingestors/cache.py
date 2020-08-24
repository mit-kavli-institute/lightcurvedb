import os
import pandas as pd
import re
from glob import glob
from sqlalchemy import create_engine, and_
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.pool import StaticPool
from sqlalchemy.sql.expression import bindparam

from lightcurvedb.core.ingestors.temp_table import (
    TemporaryQLPModel, LightcurveIDMapper, TempObservation, IngestionJob,
    TIC8Parameters, QualityFlags, FileObservation
)
from lightcurvedb.core.tic8 import TIC_Entries
from lightcurvedb.models import Lightcurve


PATHSEARCH = re.compile(
    r'orbit-(?P<orbit_number>[1-9][0-9]*)'
    r'/ffi/cam(?P<camera>[1-4])/ccd(?P<ccd>[1-4])'
    r'/LC/(?P<tic_id>[1-9][0-9]*)\.h5$'
)
DEFAULT_SCRATCH = os.path.join(
    '/',
    'scratch',
    'tmp',
    'lcdb_ingestion'
)

DEFAULT_CACHENAME = 'INGESTIONCACHE.db'


class IngestionCache(object):
    def __init__(self, scratch_dir=DEFAULT_SCRATCH, name=DEFAULT_CACHENAME):
        if not os.path.exists(scratch_dir):
            os.makedirs(scratch_dir)

        path = os.path.join(scratch_dir, name)
        self._ENGINE = create_engine(
            'sqlite:///{}'.format(path),
            connect_args={'check_same_thread': False},
            poolclass=StaticPool
        )
        TemporaryQLPModel.metadata.create_all(self._ENGINE)
        self._SESSIONFACTORY = sessionmaker(bind=self._ENGINE)
        self.session = scoped_session(self._SESSIONFACTORY)

    def clear_all_jobs(self):
        """
        Remove all jobs from the cache
        """
        self.session.query(IngestionJob).delete()

    def load_dir_to_jobs(self, directory, filter_func=None):
        search = re.search(
                r'orbit-(?P<orbit_number>[1-9][0-9]*)'
                r'/ffi/cam(?P<camera>[1-4])/ccd(?P<ccd>[1-4])',
                directory
        ).groupdict()

        orbit_number = int(search['orbit_number'])
        camera = int(search['camera'])
        ccd = int(search['ccd'])

        precheck_q = self.session.query(TempObservation).filter(
            TempObservation.orbit_number == orbit_number,
            TempObservation.camera == camera,
            TempObservation.ccd == ccd
        )

        results = []
        for temp_ops in precheck_q.all():
            results.append(temp_ops.to_dict)
        else:
            files = glob(
                os.path.join(directory, '*.h5')
            )
            for f in files:
                match = PATHSEARCH.search(f)
                if match:
                    result = match.groupdict()
                    result['file_path'] = f
                    results.append(result)

        self.session.bulk_insert_mappings(
            IngestionJob,
            results
        )

    def load_jobs(self, file_df):
        """
        Load the job table into the cache database.

        For changes to be permanent this requires the user to perform
        a subsequent ``commit``.
        """
        self.session.bulk_insert_mappings(
            IngestionJob,
            file_df.to_dict('records')
        )


    @property
    def job_tics(self):
        return self.session.query(
            IngestionJob.tic_id
        ).distinct().all()

    @property
    def quality_flag_df(self):
        q = self.session.query(
            QualityFlag.cadence.label('cadences'),
            QualityFlag.camera,
            QualityFlag.ccd,
            QualityFlag.quality_flag.label('quality_flags')
        )

        return pd.read_sql(
            q.statement,
            self.session.bind,
            index_col=['cadences', 'camera', 'ccd']
        )

    def load_observations(self, observation_df):
        """
        Load the observations into the cache database. If an entry exists,
        ignore it.

        For changes to be permanent, this requires the user to perform a
        subsequent ``commit``.
        """
        defined_tics = set(
            self.session.query(
                TempObservation.tic_id
            ).distinct().all()
        )
        new_observations = observation_df[
            ~observation_df['tic_id'].isin(defined_tics)
        ]

        if len(new_observations) > 0:
            # yay new data
            self.session.bulk_insert_mappings(
                TempObservation,
                new_observations.to_dict('records')
            )

    def remove_duplicate_jobs(self):
        """
        Remove jobs that already have observation definitions
        """
        bad_ids = self.session.query(
            IngestionJob.id
        ).join(
            TempObservation,
            and_(
                IngestionJob.tic_id == TempObservation.tic_id,
                InjectionJob.orbit_number == TempObservation.orbit_number
            )
        )
        n_bad = bad_ids.count()

        self.session.query(IngestionJob).filter(
            IngestionJob.id.in_(bad_ids.subquery())
        ).delete(synchronize_session=False)

        return n_bad, self.session.query(IngestionJob).count()

    def consolidate_lc_ids(self, lcdb):
        """
        Consolidate the ids found in the lightcurve database versus what
        exists in the cache.
        """
        tics = set(self.job_tics)
        defined_ids = set(
            self.session.query(
                LightcurveIDMapper.id
            ).all()
        )

        q = lcdb.query(
            Lightcurve.id,
            Lightcurve.tic_id,
            Lightcurve.aperture_id,
            Lightcurve.lightcurve_type_id
        ).filter(
            Lightcurve.tic_id.in_(tics),
            ~Lightcurve.id.in_(defined_ids)
        )

        new_mappers = []
        for id_, tic_id, ap_id, lc_type_id in q.yield_per(100):
            mapper = LightcurveIDMapper(
                id=id_,
                tic_id=tic_id,
                aperture_id=ap_id,
                lightcurve_type_id=lc_type_id
            )
            new_mappers.append(mapper)
        self.session.add_all(new_mappers)

    def get_observations(self, tics):
        q = self.session.query(
            FileObservation.tic_id,
            FileObservation.camera,
            FileObservation.ccd,
            FileObservation.orbit_number,
            FileObservation.file_path
        ).filter(
            FileObservation.tic_id.in_(tics)
        ).order_by(FileObservation.orbit_number)
        return pd.read_sql(
            q.statement, self.session.bind
        )


    def get_tic8_parameters(self, tics):
        q = self.session.query(
            TIC8Parameters.tic_id,
            TIC8Parameters.right_ascension,
            TIC8Parameters.declination,
            TIC8Parameters.tmag
        )
        return pd.read_sql(
            q.statement, self.session.bind,
            index_col=['tic_id']
        )

    def load_tic8_parameters(self, tic8):
        tics = set(self.job_tics)
        defined_tics = set(
            self.session.query(
                TIC8Parameters.tic_id
            ).filter(
                TIC8Parameters.tic_id.in_(tics)
            ).all()
        )
        to_consolidate = set(tics) - defined_tics

        if len(to_consolidate) > 0:
            # New parameters to query for
            q = tic8.query(
                TIC_Entries.c.id.label('tic_id'),
                TIC_Entries.c.ra.label('right_ascension'),
                TIC_Entries.c.dec.label('declination'),
                TIC_Entries.c.tmag
            )
            df = pd.read_sql(q, tic8.bind)

            self.session.bulk_insert_mappings(
                TIC8Parameters,
                df.to_dict('records')
            )

    def consolidate_quality_flags(self, quality_flag_df):
        q = self.session.query(
            QualityFlags.cadence.label('cadences'),
            QualityFlags.camera.label('cameras'),
            QualityFlags.ccd.label('ccds'),
            QualityFlags.quality_flag.label('quality_flags')
        )
        existing_flags = pd.read_sql(
            q.statement,
            self.session.bind,
            index_col=['cadences', 'cameras', 'ccds']
        )

        try:
            to_update = quality_flag_df.loc[existing_flags.index]
        except KeyError:
            to_update = []

        try:
            to_insert = quality_flag_df.loc[
                ~quality_flag_df.index.isin(to_update.index)
            ]
        except KeyError:
            to_insert = []

        update_q = QualityFlags.__table__.update().where(
            and_(
                QualityFlags.cadence == bindparam('cadences'),
                QualityFlags.camera == bindparam('cameras'),
                QualityFlags.ccd == bindparam('ccds')
            )
        ).values(
            {
                QualityFlags.quality_flag: bindparam('quality_flags')
            }
        )

        insert_q = QualityFlags.__table__.insert().values(
            {
                QualityFlags.cadence: bindparam('cadences'),
                QualityFlags.camera: bindparam('cameras'),
                QualityFlags.ccd: bindparam('ccds'),
                QualityFlags.quality_flag: bindparam('quality_flags')
            }
        )
        if len(to_update) > 0:
            self.session.execute(
                update_q,
                to_update.reset_index().to_dict('records')
            )
        if len(to_insert) > 0:
            self.session.execute(
                insert_q,
                to_insert.reset_index().to_dict('records')
            )

    def commit(self):
        self.session.commit()
