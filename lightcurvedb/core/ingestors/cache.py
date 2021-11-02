import os
import re
from glob import glob

import pandas as pd
from sqlalchemy import and_, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from sqlalchemy.sql.expression import bindparam

from lightcurvedb.core.connection import ORM_DB
from lightcurvedb.core.ingestors.temp_table import (
    FileObservation,
    IngestionJob,
    LightcurveIDMapper,
    QualityFlags,
    TempObservation,
    TemporaryQLPModel,
    TIC8Parameters,
)
from lightcurvedb.core.tic8 import TIC8_DB
from lightcurvedb.models import Lightcurve

PATHSEARCH = re.compile(
    r"orbit-(?P<orbit_number>[1-9][0-9]*)"
    r"/ffi/cam(?P<camera>[1-4])/ccd(?P<ccd>[1-4])"
    r"/LC/(?P<tic_id>[1-9][0-9]*)\.h5$"
)
DEFAULT_SCRATCH = os.path.join("/", "scratch", "tmp", "lcdb_ingestion")

DEFAULT_CACHENAME = "INGESTIONCACHE.db"


class IngestionCache(ORM_DB):
    def __init__(self, scratch_dir=DEFAULT_SCRATCH, name=DEFAULT_CACHENAME):
        if not os.path.exists(scratch_dir):
            os.makedirs(scratch_dir)

        path = os.path.join(scratch_dir, name)
        self.engine = create_engine(
            "sqlite:///{0}".format(path),
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        TemporaryQLPModel.metadata.create_all(self.engine)
        self._sessionmaker = sessionmaker(bind=self.engine)
        self._session_stack = []
        self._max_depth = 1
        self._config = path

    def clear_all_jobs(self):
        """
        Remove all jobs from the cache
        """
        self.session.query(IngestionJob).delete()

    def load_dir_to_jobs(self, directory, filter_func=None):
        search = re.search(
            r"orbit-(?P<orbit_number>[1-9][0-9]*)"
            r"/ffi/cam(?P<camera>[1-4])/ccd(?P<ccd>[1-4])",
            directory,
        ).groupdict()

        orbit_number = int(search["orbit_number"])
        camera = int(search["camera"])
        ccd = int(search["ccd"])

        precheck_q = self.session.query(TempObservation).filter(
            TempObservation.orbit_number == orbit_number,
            TempObservation.camera == camera,
            TempObservation.ccd == ccd,
        )

        results = []
        for temp_ops in precheck_q.all():
            results.append(temp_ops.to_dict)
        else:
            files = glob(os.path.join(directory, "*.h5"))
            for f in files:
                match = PATHSEARCH.search(f)
                if match:
                    result = match.groupdict()
                    result["file_path"] = f
                    results.append(result)

        self.session.bulk_insert_mappings(IngestionJob, results)

    def load_jobs(self, file_df):
        """
        Load the job table into the cache database.

        For changes to be permanent this requires the user to perform
        a subsequent ``commit``.
        """
        self.session.bulk_insert_mappings(
            IngestionJob, file_df.to_dict("records")
        )

    @property
    def job_tics(self):
        return self.session.query(IngestionJob.tic_id).distinct().all()

    @property
    def quality_flag_df(self):
        q = self.session.query(
            QualityFlags.cadence,
            QualityFlags.camera,
            QualityFlags.ccd,
            QualityFlags.quality_flag,
        )

        return pd.read_sql(
            q.statement,
            self.session.bind,
            index_col=["camera", "ccd", "cadence"],
        ).sort_index()

    @property
    def quality_flag_map(self):
        cams_and_ccds = (
            self.query(QualityFlags.camera, QualityFlags.ccd).distinct().all()
        )
        mapping = {}
        for cam, ccd in cams_and_ccds:
            q = self.query(
                QualityFlags.cadence, QualityFlags.quality_flag
            ).filter_by(camera=cam, ccd=ccd)
            df = pd.read_sql(
                q.statement, self.session.bind, index_col="cadence"
            ).sort_index()
            mapping[(cam, ccd)] = df
        return mapping

    @property
    def tic_parameter_df(self):
        q = self.session.query(
            TIC8Parameters.tic_id,
            TIC8Parameters.tmag,
            TIC8Parameters.right_ascension,
            TIC8Parameters.declination,
        )

        tic_parameters = pd.read_sql(
            q.statement, self.session.bind, index_col=["tic_id"]
        )
        return tic_parameters.sort_index()

    @property
    def tic_parameter_map(self):
        q = self.session.query(
            TIC8Parameters.tic_id,
            TIC8Parameters.tmag,
            TIC8Parameters.right_ascension,
            TIC8Parameters.declination,
        )
        result = {}
        for tic_id, tmag, right_ascension, declination in q:
            result[tic_id] = {
                "tmag": tmag,
                "ra": right_ascension,
                "dec": declination,
            }
        return result

    @property
    def file_observation_df(self):
        q = self.session.query(
            FileObservation.tic_id,
            FileObservation.file_path,
            FileObservation.camera,
            FileObservation.ccd,
            FileObservation.file_path,
        )

        return pd.read_sql(q.statement, self.session.bind)

    def load_observations(self, observation_df):
        """
        Load the observations into the cache database. If an entry exists,
        ignore it.

        For changes to be permanent, this requires the user to perform a
        subsequent ``commit``.
        """
        defined_tics = set(
            self.session.query(TempObservation.tic_id).distinct().all()
        )
        new_observations = observation_df[
            ~observation_df["tic_id"].isin(defined_tics)
        ]

        if len(new_observations) > 0:
            # yay new data
            self.session.bulk_insert_mappings(
                TempObservation, new_observations.to_dict("records")
            )

    def remove_duplicate_jobs(self):
        """
        Remove jobs that already have observation definitions
        """
        bad_ids = self.session.query(IngestionJob.id).join(
            TempObservation,
            and_(
                IngestionJob.tic_id == TempObservation.tic_id,
                IngestionJob.orbit_number == TempObservation.orbit_number,
            ),
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
        defined_ids = set(self.session.query(LightcurveIDMapper.id).all())

        q = lcdb.query(
            Lightcurve.id,
            Lightcurve.tic_id,
            Lightcurve.aperture_id,
            Lightcurve.lightcurve_type_id,
        ).filter(Lightcurve.tic_id.in_(tics), ~Lightcurve.id.in_(defined_ids))

        new_mappers = []
        for id_, tic_id, ap_id, lc_type_id in q.yield_per(100):
            mapper = LightcurveIDMapper(
                id=id_,
                tic_id=tic_id,
                aperture_id=ap_id,
                lightcurve_type_id=lc_type_id,
            )
            new_mappers.append(mapper)
        self.session.add_all(new_mappers)

    def get_observations(self, tics):
        q = (
            self.session.query(
                FileObservation.tic_id,
                FileObservation.camera,
                FileObservation.ccd,
                FileObservation.orbit_number,
                FileObservation.file_path,
            )
            .filter(FileObservation.tic_id.in_(tics))
            .order_by(FileObservation.orbit_number)
        )
        return pd.read_sql(q.statement, self.session.bind)

    def get_tic8_parameters(self, tics):
        q = self.session.query(
            TIC8Parameters.tic_id,
            TIC8Parameters.right_ascension,
            TIC8Parameters.declination,
            TIC8Parameters.tmag,
        )
        return pd.read_sql(
            q.statement, self.session.bind, index_col=["tic_id"]
        )

    def load_tic8_parameters(self):
        tics = set(self.job_tics)
        defined_tics = set(
            self.session.query(TIC8Parameters.tic_id)
            .filter(TIC8Parameters.tic_id.in_(tics))
            .all()
        )

        to_consolidate = set(tics) - defined_tics
        if len(to_consolidate) > 0:
            # New parameters to query for

            with TIC8_DB as tic8:
                q = tic8.query(
                    tic8.ticentries.c.id.label("tic_id"),
                    tic8.ticentries.c.ra.label("right_ascension"),
                    tic8.ticentries.c.dec.label("declination"),
                    tic8.ticentries.c.tmag,
                ).filter(tic8.ticentries.c.id.in_(to_consolidate))
                df = pd.read_sql(q.statement, tic8.bind)

        self.session.bulk_insert_mappings(
            TIC8Parameters, df.to_dict("records")
        )

    def consolidate_quality_flags(self, quality_flag_df):
        q = self.session.query(
            QualityFlags.cadence.label("cadences"),
            QualityFlags.camera.label("cameras"),
            QualityFlags.ccd.label("ccds"),
            QualityFlags.quality_flag.label("quality_flags"),
        )
        existing_flags = pd.read_sql(
            q.statement,
            self.session.bind,
            index_col=["cadences", "cameras", "ccds"],
        )

        try:
            to_update = quality_flag_df[
                quality_flag_df.index.isin(existing_flags.index)
            ]
        except KeyError:
            to_update = []

        try:
            to_insert = quality_flag_df[
                ~quality_flag_df.index.isin(existing_flags.index)
            ]
        except KeyError:
            to_insert = []

        update_q = (
            QualityFlags.__table__.update()
            .where(
                and_(
                    QualityFlags.cadence == bindparam("cadences"),
                    QualityFlags.camera == bindparam("cameras"),
                    QualityFlags.ccd == bindparam("ccds"),
                )
            )
            .values({QualityFlags.quality_flag: bindparam("quality_flags")})
        )

        insert_q = QualityFlags.__table__.insert().values(
            {
                QualityFlags.cadence: bindparam("cadences"),
                QualityFlags.camera: bindparam("cameras"),
                QualityFlags.ccd: bindparam("ccds"),
                QualityFlags.quality_flag: bindparam("quality_flags"),
            }
        )
        if len(to_update) > 0:
            self.session.execute(
                update_q, to_update.reset_index().to_dict("records")
            )
        if len(to_insert) > 0:
            self.session.execute(
                insert_q, to_insert.reset_index().to_dict("records")
            )
        return len(to_update), len(to_insert)
