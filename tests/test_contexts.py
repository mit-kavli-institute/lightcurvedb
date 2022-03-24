import tempfile
import pathlib
from lightcurvedb.core.ingestors import contexts
from hypothesis import strategies as st, given
from .strategies import tess as tess_st


TIC_PARAM_ORDER = ("tic_id", "ra", "dec", "tmag", "pmra", "pmdec", "jmag", "kmag", "vmag")
QUALITY_FLAG_PARAM_ORDER = ("cadence", "camera", "ccd", "quality_flag")


def _dump_txt(path, data, param_order, formatter=str):
    with open(path, "wt") as fout:
        for row in data:
            _raw = tuple(row[key] for key in param_order)
            fout.write("\t".join(map(formatter, _raw)))
            fout.write("\n")
    return path


@given(
    st.lists(tess_st.tic_parameters(), unique_by=lambda p: p["tic_id"])
)
def test_tic_catalog_loading(parameters):
    sqlite_name = pathlib.Path("db.sqlite")
    catalog_name = pathlib.Path("catalog.txt")

    with tempfile.TemporaryDirectory() as tmpdir:
        sqlite_path = pathlib.Path(tmpdir) / sqlite_name
        catalog_path = pathlib.Path(tmpdir) / catalog_name

        _dump_txt(catalog_path, parameters, TIC_PARAM_ORDER)

        contexts.populate_tic_catalog(sqlite_path, catalog_path)

        for param in parameters:
            remote = contexts.get_tic_parameters(
                param["tic_id"],
                *tuple(param.keys())
            )
            for key in TIC_PARAM_ORDER:
                assert param[key] == remote[key]
