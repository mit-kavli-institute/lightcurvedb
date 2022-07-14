import tempfile
import pathlib
import numpy as np
from lightcurvedb.core.ingestors import contexts
from hypothesis import strategies as st, given, settings, note
from .strategies import tess as tess_st


TIC_PARAM_ORDER = ("tic_id", "ra", "dec", "tmag", "pmra", "pmdec", "jmag", "kmag", "vmag")

def _dump_tuple_txt(path, data, formatter=str):
    with open(path, "wt") as fout:
        for row in data:
            fout.write("\t".join(map(formatter, row)))
            fout.write("\n")
    return path

def _dump_dict_txt(path, data, param_order, formatter=str):
    return _dump_tuple_txt(
        path,
        (tuple(row[key] for key in param_order) for row in data),
        formatter=formatter
    )


@settings(deadline=None)
@given(
    st.lists(tess_st.tic_parameters(), unique_by=lambda p: p["tic_id"])
)
def test_tic_catalog_loading(parameters):
    sqlite_name = pathlib.Path("db.sqlite")
    catalog_name = pathlib.Path("catalog.txt")

    with tempfile.TemporaryDirectory() as tmpdir:
        sqlite_path = pathlib.Path(tmpdir) / sqlite_name
        catalog_path = pathlib.Path(tmpdir) / catalog_name

        contexts.make_shared_context(sqlite_path)

        _dump_dict_txt(catalog_path, parameters, TIC_PARAM_ORDER)

        contexts.populate_tic_catalog(sqlite_path, catalog_path)

        for param in parameters:
            remote = contexts.get_tic_parameters(
                sqlite_path,
                param["tic_id"],
                *tuple(param.keys())
            )
            for key in TIC_PARAM_ORDER:
                ref = param[key]
                if np.isnan(ref):
                    assert np.isnan(remote[key])
                else:
                    assert param[key] == remote[key]


@settings(deadline=None)
@given(
    st.lists(
        st.tuples(
            tess_st.cadences(),
            tess_st.quality_flags()
        ),
        unique_by=lambda r: r[0]
    ),
    tess_st.cameras(),
    tess_st.ccds()
)
def test_quality_flag_loading(quality_flags, camera, ccd):
    sqlite_name = pathlib.Path("db.sqlite")
    qflag_name = pathlib.Path("quality_flags.txt")

    with tempfile.TemporaryDirectory() as tmpdir:
        sqlite_path = pathlib.Path(tmpdir) / sqlite_name
        qflag_path = pathlib.Path(tmpdir) / qflag_name

        contexts.make_shared_context(sqlite_path)

        _dump_tuple_txt(
            qflag_path,
            quality_flags
        )
        contexts.populate_quality_flags(sqlite_path, qflag_path, camera, ccd)
        for cadence, quality_flag in quality_flags:
            remote_flag = contexts.get_qflag(sqlite_path, cadence, camera, ccd)
            assert remote_flag == quality_flag


@given(
    st.lists(
        st.tuples(
            tess_st.cadences(),
            tess_st.quality_flags()
        ),
        unique_by=lambda r: r[0]
    ),
    tess_st.cameras(),
    tess_st.ccds()
)
def test_quality_flag_ordering(quality_flags, camera, ccd):
    sqlite_name = pathlib.Path("db.sqlite")
    qflag_name = pathlib.Path("quality_flags.txt")

    with tempfile.TemporaryDirectory() as tmpdir:
        sqlite_path = pathlib.Path(tmpdir) / sqlite_name
        qflag_path = pathlib.Path(tmpdir) / qflag_name

        contexts.make_shared_context(sqlite_path)

        _dump_tuple_txt(
            qflag_path,
            quality_flags
        )

        contexts.populate_quality_flags(sqlite_path, qflag_path, camera, ccd)
        np_arr = contexts.get_qflag_np(sqlite_path, camera, ccd)
        note(np_arr)
        for ith, row in enumerate(sorted(quality_flags, key=lambda r: r[0])):
            remote_row = np_arr[ith]
            note(f"{row} {remote_row}")
            assert row[0] == remote_row[0]
            assert row[1] == remote_row[1]
