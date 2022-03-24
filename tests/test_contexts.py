import tempfile
import pathlib
from lightcurvedb.core.ingestors import contexts
from hypothesis import strategies as st, given
from numpy import isnan
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
                if isnan(ref):
                    assert isnan(remote[key])
                else:
                    assert param[key] == remote[key]


@given(
    st.lists(
        st.tuples(
            tess_st.cadences(),
            tess_st.cameras(),
            tess_st.ccds(),
            tess_st.quality_flags()
        ),
        unique_by=lambda r: (r[0], r[1], r[2])
    )
)
def test_quality_flag_loading(quality_flags):
    sqlite_name = pathlib.Path("db.sqlite")
    qflag_name = pathlib.Path("quality_flags.txt")

 
