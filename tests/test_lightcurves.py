import itertools
import pathlib
from tempfile import TemporaryDirectory

import numpy as np
from hypothesis import HealthCheck, assume, given, note, settings
from hypothesis import strategies as st

from lightcurvedb import models
from lightcurvedb.core.ingestors import contexts
from lightcurvedb.core.ingestors.correction import LightcurveCorrector
from lightcurvedb.core.ingestors.jobs import DirectoryPlan
from lightcurvedb.core.ingestors.lightcurves import h5_to_numpy
from lightcurvedb.core.ingestors.lightpoints import (
    ExponentialSamplingLightpointIngestor,
)

from .strategies import ingestion
from .strategies import orm as orm_st

FORBIDDEN_KEYWORDS = (
    "\x00",
    "/",
    "X",
    "Y",
    ".",
    "Cadence",
    "BJD",
    "QualityFlag",
    "LightCurve",
    "AperturePhotometry",
)


@given(
    st.lists(st.text(min_size=1), min_size=1, max_size=5, unique=True),
    st.lists(st.text(min_size=1), min_size=1, max_size=5, unique=True),
    st.integers(min_value=0, max_value=2 ** 63),
    st.data(),
)
def test_h5_to_numpy(apertures, lightcurve_types, lightcurve_id, data):
    assume(
        all(
            all(keyword not in name for keyword in FORBIDDEN_KEYWORDS)
            for name in itertools.chain(apertures, lightcurve_types)
        )
    )
    with TemporaryDirectory() as tempdir:
        h5_path, data_gen_ref = ingestion.simulate_h5_file(
            data,
            tempdir,
            "BackgroundAperture",
            "Background",
            apertures,
            lightcurve_types,
        )
        bg = data_gen_ref["background"]
        for ap, lc_t in itertools.product(apertures, lightcurve_types):
            check = h5_to_numpy(lightcurve_id, ap, lc_t, h5_path)
            ref = data_gen_ref["photometry"][(ap, lc_t)]
            np.testing.assert_array_equal(check["cadence"], bg["cadence"])
            np.testing.assert_array_equal(
                check["barycentric_julian_date"],
                bg["barycentric_julian_date"][0],
            )
            np.testing.assert_array_equal(check["data"], ref["data"][0])
            np.testing.assert_array_equal(check["error"], ref["error"][0])


@settings(
    deadline=None,
    suppress_health_check=(
        HealthCheck.too_slow,
        HealthCheck.function_scoped_fixture,
    ),
)
@given(st.data())
def test_corrector_instantiation(db, data):
    with TemporaryDirectory() as tempdir, db:
        (
            run_path,
            directory,
        ) = ingestion.simulate_lightcurve_ingestion_environment(
            data, tempdir, db
        )
        frame_type = db.query(models.FrameType.name).first()[0]
        cache_path = pathlib.Path(tempdir, "db.sqlite3")
        contexts.make_shared_context(cache_path)
        contexts.populate_ephemeris(cache_path, db)
        contexts.populate_tjd_mapping(
            cache_path,
            db,
            frame_type=frame_type
        )

        plan = DirectoryPlan([directory], db)
        catalog_template = (
            str(run_path)
            + "/catalog_{orbit_number}_{camera}_{ccd}_full.txt"
        )
        quality_template = str(run_path) + "/cam{camera}ccd{ccd}_qflag.txt"

        for catalog in plan.yield_needed_tic_catalogs(
            path_template=catalog_template
        ):
            contexts.populate_tic_catalog(cache_path, catalog)
        for args in plan.yield_needed_quality_flags(
            path_template=quality_template
        ):
            contexts.populate_quality_flags(cache_path, *args)

        corrector = LightcurveCorrector(cache_path)


@settings(
    deadline=None,
    suppress_health_check=(
        HealthCheck.too_slow,
        HealthCheck.function_scoped_fixture,
    ),
)
@given(st.data())
def test_ingestor_instantiation(db, data):
    try:
        with TemporaryDirectory() as tempdir, db:
            (
                run_path,
                directory,
            ) = ingestion.simulate_lightcurve_ingestion_environment(
                data, tempdir, db
            )
            frame_type = db.query(models.FrameType.name).first()[0]
            cache_path = pathlib.Path(tempdir, "db.sqlite3")
            contexts.make_shared_context(cache_path)
            contexts.populate_ephemeris(cache_path, db)
            contexts.populate_tjd_mapping(cache_path, db, frame_type=frame_type)

            plan = DirectoryPlan([directory], db)
            catalog_template = (
                str(run_path)
                + "/catalog_{orbit_number}_{camera}_{ccd}_full.txt"
            )
            quality_template = str(run_path) + "/cam{camera}ccd{ccd}_qflag.txt"

            for catalog in plan.yield_needed_tic_catalogs(
                path_template=catalog_template
            ):
                contexts.populate_tic_catalog(cache_path, catalog)
            for args in plan.yield_needed_quality_flags(
                path_template=quality_template
            ):
                contexts.populate_quality_flags(cache_path, *args)

            process = ExponentialSamplingLightpointIngestor(
                db._config, "test-worker", None, 0, cache_path
            )
            assert process is not None

    finally:
        with db:
            opt = {"synchronize_session": False}
            db.query(models.Lightpoint).delete(**opt)
            db.query(models.Lightcurve).delete(**opt)
            db.query(models.Observation).delete(**opt)
            db.query(models.Frame).delete(**opt)
            db.query(models.FrameType).delete(**opt)
            db.query(models.LightcurveType).delete(**opt)
            db.query(models.Aperture).delete(**opt)
            db.query(models.Orbit).delete(**opt)
            db.query(models.SpacecraftEphemeris).delete(**opt)
            db.commit()


# @settings(
#     deadline=None,
#     suppress_health_check=(
#         HealthCheck.too_slow,
#         HealthCheck.function_scoped_fixture,
#     ),
# )
# @given(st.data())
# def test_ingestor_processing(db, data):
#     try:
#         with TemporaryDirectory() as tempdir:
#             with db:
#                 (
#                     run_path,
#                     directory,
#                 ) = ingestion.simulate_lightcurve_ingestion_environment(
#                     data, tempdir, db
#                 )
#                 frame_type = db.query(models.FrameType).first().name
#                 cache_path = pathlib.Path(tempdir, "db.sqlite3")
#                 contexts.make_shared_context(cache_path)
#                 contexts.populate_ephemeris(cache_path, db)
#                 contexts.populate_tjd_mapping(
#                     cache_path, db, frame_type=frame_type
#                 )
# 
#                 note(contexts.get_tjd_mapping(cache_path))
# 
#                 plan = DirectoryPlan([directory], db)
#                 jobs = plan.get_jobs()
#                 catalog_template = (
#                     str(run_path)
#                     + "/catalog_{orbit_number}_{camera}_{ccd}_full.txt"
#                 )
#                 quality_template = (
#                     str(run_path) + "/cam{camera}ccd{ccd}_qflag.txt"
#                 )
# 
#                 for catalog in plan.yield_needed_tic_catalogs(
#                     path_template=catalog_template
#                 ):
#                     contexts.populate_tic_catalog(cache_path, catalog)
#                 for args in plan.yield_needed_quality_flags(
#                     path_template=quality_template
#                 ):
#                     contexts.populate_quality_flags(cache_path, *args)
# 
#                 class MockQueue:
#                     def task_done(self):
#                         pass
# 
#                 stage_id = db.query(models.QLPStage.id).first()[0]
#                 db.commit()
#             process = ExponentialSamplingLightpointIngestor(
#                 db._config, "test-worker", MockQueue(), stage_id, cache_path
#             )
# 
#             process._create_db()
#             process._load_contexts()
#             with process.db as db:
#                 for job in jobs:
#                     process.process_job(job)
#                     process.flush(db)
#             with db:
#                 assert db.query(models.Lightpoint).count() > 0
# 
#     finally:
#         with db:
#             opt = {"synchronize_session": False}
#             db.query(models.Lightpoint).delete(**opt)
#             db.query(models.Lightcurve).delete(**opt)
#             db.query(models.Observation).delete(**opt)
#             db.query(models.Frame).delete(**opt)
#             db.query(models.FrameType).delete(**opt)
#             db.query(models.LightcurveType).delete(**opt)
#             db.query(models.Aperture).delete(**opt)
#             db.query(models.Orbit).delete(**opt)
#             db.query(models.SpacecraftEphemeris).delete(**opt)
#             db.commit()
