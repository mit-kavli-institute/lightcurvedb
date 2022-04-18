import tempfile
import pathlib
import numpy as np
from hypothesis import given, strategies as st

from lightcurvedb.core.ingestors import contexts as ctx

from .strategies import tess as tess_st, orm as orm_st

CATALOG_KEY_ORDER = (
    "tic_id",
    "ra",
    "dec",
    "tmag",
    "pmra",
    "pmdec",
    "jmag",
    "kmag",
    "vmag"
)


def _simulate_tic_catalog(data, directory):
    filename = data.draw(
        st.from_regex(r"^catalog_[0-9]+_[1-4]_[1-4]_(bright|full)\.txt$")
    )

    tic_parameters = data.draw(
        st.lists(
            tess_st.tic_parameters(),
            unique_by=lambda param: param["tic_id"]
        )
    )

    with open(directory / pathlib.Path(filename), "wt") as fout:
        for param in tic_parameters:
            msg = " ".join(map(str, (param[key] for key in CATALOG_KEY_ORDER)))
            fout.write(msg)
            fout.write("\n")
    return directory / pathlib.Path(filename), tic_parameters


def _simulate_quality_flag_file(data, directory):
    camera = data.draw(tess_st.cameras())
    ccd = data.draw(tess_st.ccds())

    quality_flags = data.draw(
        st.lists(
            st.tuples(
                tess_st.cadences(),
                st.integers(min_value=0, max_value=1)
            ),
            unique_by=lambda qflag: qflag[0]
        )
    )

    filename = pathlib.Path(
        f"cam{camera}ccd{ccd}_qflag.txt"
    )

    with open(directory / filename, "wt") as fout:
        for cadence, flag in quality_flags:
            fout.write(f"{cadence} {flag}\n")

    return directory / filename, camera, ccd, quality_flags


@given(st.data())
def test_tic_catalog_caching(data):

    columns = data.draw(
        st.lists(
            st.sampled_from(CATALOG_KEY_ORDER),
            min_size=1,
            max_size=len(CATALOG_KEY_ORDER)
        )
    )

    with tempfile.TemporaryDirectory() as tempdir:
        db_path = pathlib.Path(tempdir) / pathlib.Path("db.sqlite3")
        ctx.make_shared_context(db_path)
        catalog_path, parameter_ref = _simulate_tic_catalog(data, tempdir)
        ctx.populate_tic_catalog(db_path, catalog_path)

        for ref in parameter_ref:
            tic_id = ref["tic_id"]

            check = ctx.get_tic_parameters(
                db_path,
                tic_id,
                *columns
            )

            for col in columns:
                if np.isnan(ref[col]):
                    assert np.isnan(check[col])
                else:
                    assert ref[col] == check[col]


@given(st.data())
def test_tic_catalog_mapping(data):

    columns = data.draw(
        st.lists(
            st.sampled_from(CATALOG_KEY_ORDER),
            min_size=1,
            max_size=len(CATALOG_KEY_ORDER)
        )
    )

    with tempfile.TemporaryDirectory() as tempdir:
        db_path = pathlib.Path(tempdir) / pathlib.Path("db.sqlite3")
        ctx.make_shared_context(db_path)
        catalog_path, parameter_ref = _simulate_tic_catalog(data, tempdir)
        ctx.populate_tic_catalog(db_path, catalog_path)

        parameter_ref = {row["tic_id"]: row for row in parameter_ref}

        tic_mapping = ctx.get_tic_mapping(
            db_path,
            *columns
        )

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

        path, camera, ccd, quality_flag_ref = _simulate_quality_flag_file(
            data,
            tempdir
        )
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

        path, camera, ccd, quality_flag_ref = _simulate_quality_flag_file(
            data,
            tempdir
        )
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
        orm_st.spacecraft_ephemris(),
        min_size=1,
        unique_by=lambda eph: eph.barycentric_dynamical_time
    )
)
def test_spacecraft_eph_cache(database, eph_list):
    with database as db, tempfile.TemporaryDirectory() as tempdir:
        db_path = pathlib.Path(tempdir) / pathlib.Path("db.sqlite3")

        db.session.add_all(eph_list)
        db.flush()

        ctx.make_shared_context(db_path)
        ctx.populate_ephemris(db_path, db)

        ref_eph = sorted(eph_list, key=lambda eph: eph.barycentric_dynamical_time)
        ref_x = np.array([eph.x for eph in ref_eph])
        ref_y = np.array([eph.y for eph in ref_eph])
        ref_z = np.array([eph.z for eph in ref_eph])

        check_x = ctx.get_spacecraft_data(db_path, "x")
        check_y = ctx.get_spacecraft_data(db_path, "y")
        check_z = ctx.get_spacecraft_data(db_path, "z")

        assert np.array_equal(check_x, ref_x)
        assert np.array_equal(check_y, ref_y)
        assert np.array_equal(check_z, ref_z)
