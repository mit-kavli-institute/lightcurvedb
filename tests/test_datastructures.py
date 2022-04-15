import tempfile
import pathlib
import numpy as np
from hypothesis import given, strategies as st

from lightcurvedb.core.ingestors import contexts as ctx

from .strategies import tess as tess_st

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
