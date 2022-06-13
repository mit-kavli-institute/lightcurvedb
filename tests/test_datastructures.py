import pathlib
import tempfile

import numpy as np
from hypothesis import given
from hypothesis import strategies as st

from lightcurvedb.core.ingestors import contexts as ctx

from .strategies import ingestion
from .strategies import orm as orm_st


@given(st.data())
def test_tic_catalog_mapping(data):

    columns = data.draw(
        st.lists(
            st.sampled_from(ingestion.CATALOG_KEY_ORDER),
            min_size=1,
            max_size=len(ingestion.CATALOG_KEY_ORDER),
        )
    )

    with tempfile.TemporaryDirectory() as tempdir:
        db_path = pathlib.Path(tempdir) / pathlib.Path("db.sqlite3")
        ctx.make_shared_context(db_path)
        catalog_path, parameter_ref = ingestion.simulate_tic_catalog(
            data, tempdir
        )
        ctx.populate_tic_catalog(db_path, catalog_path)

        parameter_ref = {row["tic_id"]: row for row in parameter_ref}

        tic_mapping = ctx.get_tic_mapping(db_path, *columns)

        for tic_id, values in parameter_ref.items():
            check = tic_mapping[tic_id]

            assert set(check.keys()) == set(columns)
            for key in columns:
                if np.isnan(values[key]):
                    assert np.isnan(check[key])
                else:
                    assert check.get(key, None) == values[key]


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
            check == flag


@given(
    orm_st.database(),
    st.lists(
        orm_st.spacecraft_ephemeris(),
        min_size=1,
        unique_by=lambda eph: eph.barycentric_dynamical_time,
    ),
)
def test_spacecraft_eph_cache(database, eph_list):
    with database as db, tempfile.TemporaryDirectory() as tempdir:
        db_path = pathlib.Path(tempdir) / pathlib.Path("db.sqlite3")

        db.session.add_all(eph_list)
        db.flush()

        ctx.make_shared_context(db_path)
        ctx.populate_ephemeris(db_path, db)

        ref_eph = sorted(
            eph_list, key=lambda eph: eph.barycentric_dynamical_time
        )
        ref_x = np.array([eph.x for eph in ref_eph])
        ref_y = np.array([eph.y for eph in ref_eph])
        ref_z = np.array([eph.z for eph in ref_eph])

        check_x = ctx.get_spacecraft_data(db_path, "x")
        check_y = ctx.get_spacecraft_data(db_path, "y")
        check_z = ctx.get_spacecraft_data(db_path, "z")

        assert np.array_equal(check_x, ref_x)
        assert np.array_equal(check_y, ref_y)
        assert np.array_equal(check_z, ref_z)


@given(
    orm_st.database(),
    orm_st.frame_types(name=st.just("Raw FFI")),
    orm_st.orbits(),
    st.lists(
        orm_st.frames(ccd=st.just(None)),
        min_size=1,
        unique_by=(lambda f: (f.cadence, f.camera), lambda f: f.file_path),
    ),
)
def test_tjd_cache(database, raw_ffi_type, orbit, frames):
    with database as db, tempfile.TemporaryDirectory() as tempdir:
        cache_path = pathlib.Path(tempdir) / pathlib.Path("db.sqlite3")
        db.add(raw_ffi_type)
        db.add(orbit)
        for frame in frames:
            frame.frame_type = raw_ffi_type
            frame.orbit = orbit
        db.session.add_all(frames)
        db.flush()

        ctx.make_shared_context(cache_path)
        ctx.populate_tjd_mapping(cache_path, db)

        tjd_check_df = ctx.get_tjd_mapping(cache_path)
        for frame_ref in frames:
            check = tjd_check_df.loc[
                (frame_ref.camera, frame_ref.cadence)
            ].iloc[0]
            if np.isnan(frame_ref.mid_tjd):
                assert np.isnan(check)
            else:
                assert check == frame_ref.mid_tjd
