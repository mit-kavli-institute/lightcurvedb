import pathlib
import tempfile

from hypothesis import given, settings
from hypothesis import strategies as st

from lightcurvedb.core.ingestors import contexts as ctx

from .strategies import ingestion


@settings(deadline=None)
@given(st.data())
def test_quality_flag_cache(data):
    with tempfile.TemporaryDirectory() as tempdir:
        db_path = pathlib.Path(tempdir) / pathlib.Path("db.sqlite3")

        (
            path,
            camera,
            ccd,
            quality_flag_ref,
        ) = ingestion.simulate_quality_flag_file(data, tempdir)
        ctx.make_shared_context(db_path)
        ctx.populate_quality_flags(db_path, path, camera, ccd)

        for cadence, flag in quality_flag_ref:
            check = ctx.get_qflag(
                db_path,
                cadence,
                camera,
                ccd,
            )
            check == flag


@settings(deadline=None)
@given(st.data())
def test_quality_flag_np(data):
    with tempfile.TemporaryDirectory() as tempdir:
        db_path = pathlib.Path(tempdir) / pathlib.Path("db.sqlite3")

        (
            path,
            camera,
            ccd,
            quality_flag_ref,
        ) = ingestion.simulate_quality_flag_file(data, tempdir)
        ctx.make_shared_context(db_path)
        ctx.populate_quality_flags(db_path, path, camera, ccd)

        for cadence, flag in quality_flag_ref:
            check = ctx.get_qflag(
                db_path,
                cadence,
                camera,
                ccd,
            )
            assert check == flag
