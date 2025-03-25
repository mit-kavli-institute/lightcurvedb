import pathlib
import tempfile

from hypothesis import given, note, settings
from hypothesis import strategies as st

from lightcurvedb.core.ingestors import contexts

from .strategies import tess as tess_st

TIC_PARAM_ORDER = (
    "tic_id",
    "ra",
    "dec",
    "tmag",
    "pmra",
    "pmdec",
    "jmag",
    "kmag",
    "vmag",
)


def _dump_tuple_txt(path, data, formatter=str):
    with open(path, "wt") as fout:
        for row in data:
            fout.write("\t".join(map(formatter, row)))
            fout.write("\n")
    return path


@settings(deadline=None)
@given(
    st.lists(
        st.tuples(tess_st.cadences(), tess_st.quality_flags()),
        unique_by=lambda r: r[0],
    ),
    tess_st.cameras(),
    tess_st.ccds(),
)
def test_quality_flag_loading(quality_flags, camera, ccd):
    sqlite_name = pathlib.Path("db.sqlite")
    qflag_name = pathlib.Path("quality_flags.txt")

    with tempfile.TemporaryDirectory() as tmpdir:
        sqlite_path = pathlib.Path(tmpdir) / sqlite_name
        qflag_path = pathlib.Path(tmpdir) / qflag_name

        contexts.make_shared_context(sqlite_path)

        _dump_tuple_txt(qflag_path, quality_flags)
        contexts.populate_quality_flags(sqlite_path, qflag_path, camera, ccd)
        for cadence, quality_flag in quality_flags:
            remote_flag = contexts.get_qflag(sqlite_path, cadence, camera, ccd)
            assert remote_flag == quality_flag


@settings(deadline=None)
@given(
    st.lists(
        st.tuples(tess_st.cadences(), tess_st.quality_flags()),
        unique_by=lambda r: r[0],
    ),
    tess_st.cameras(),
    tess_st.ccds(),
)
def test_quality_flag_ordering(quality_flags, camera, ccd):
    sqlite_name = pathlib.Path("db.sqlite")
    qflag_name = pathlib.Path("quality_flags.txt")

    with tempfile.TemporaryDirectory() as tmpdir:
        sqlite_path = pathlib.Path(tmpdir) / sqlite_name
        qflag_path = pathlib.Path(tmpdir) / qflag_name

        contexts.make_shared_context(sqlite_path)

        _dump_tuple_txt(qflag_path, quality_flags)

        contexts.populate_quality_flags(sqlite_path, qflag_path, camera, ccd)
        np_arr = contexts.get_qflag_np(sqlite_path, camera, ccd)
        note(np_arr)
        for ith, row in enumerate(sorted(quality_flags, key=lambda r: r[0])):
            remote_row = np_arr[ith]
            note(f"{row} {remote_row}")
            assert row[0] == remote_row[0]
            assert row[1] == remote_row[1]
