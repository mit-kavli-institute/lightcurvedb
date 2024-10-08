import pathlib
from tempfile import TemporaryDirectory

import pytest
import sqlalchemy as sa
from hypothesis import HealthCheck, given, note, settings
from hypothesis import strategies as st

from lightcurvedb import models
from lightcurvedb.core.ingestors import contexts, lightcurve_arrays
from lightcurvedb.core.ingestors.jobs import DirectoryPlan
from lightcurvedb.models.lightcurve import ArrayOrbitLightcurve

from .strategies import ingestion

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


@settings(
    deadline=None,
    suppress_health_check=(
        HealthCheck.too_slow,
        HealthCheck.function_scoped_fixture,
    ),
)
@given(st.data())
def test_corrector_instantiation(isolated_database, data):
    with TemporaryDirectory() as tempdir, isolated_database as db:
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

        plan = DirectoryPlan([directory], db.config)
        catalog_template = (
            str(run_path) + "/catalog_{orbit_number}_{camera}_{ccd}_full.txt"
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

        assert plan is not None


@settings(
    deadline=None,
    suppress_health_check=(
        HealthCheck.too_slow,
        HealthCheck.function_scoped_fixture,
    ),
)
@pytest.mark.timeout(60)
@given(st.data())
def test_ingest(isolated_database, data):
    with TemporaryDirectory() as tempdir, isolated_database as db:
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

        plan = DirectoryPlan([directory], db.config)
        catalog_template = (
            str(run_path) + "/catalog_{orbit_number}_{camera}_{ccd}_full.txt"
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

        jobs = plan.get_jobs()
        lightcurve_arrays.ingest_jobs(
            {
                "log_level": "debug",
                "dbconf": str(isolated_database.config),
                "n_processes": 1,
            },
            jobs,
            cache_path,
        )

        q = sa.select(
            sa.func.count(ArrayOrbitLightcurve.tic_id).label("n_lightcurves")
        )
        n_lightcurves = db.scalar(q)
        note(str(list(directory.glob("*"))))
        assert n_lightcurves is not None
        assert n_lightcurves > 0
