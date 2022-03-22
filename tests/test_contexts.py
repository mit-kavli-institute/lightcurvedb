from hypothesis import strategies as st, given
from lightcurvedb.core import sql
from lightcurvedb.core.ingestors.contexts import DataStructure, IngestionContext
from sqlalchemy import MetaData


TEST_META = MetaData()


@st.composite
def types(draw):
    return st.tuples(
        *tuple(
            st.just(k) for k in sql._SQL_ALIASES.keys()
        )
    )


@given(
    st.text(min_size=1),
    st.data()
)
def test_datastructures(name, data):
    ctx = IngestionContext()
    idx_cols = data.draw(st.lists(types, min_size=1, max_size=3))
    data_cols = data.draw(st.lists(types, max_size=3))

    ds = ctx.data_structure(name)

    for i, idx in enumerate(idx_cols):
        ds.add_key(f"index-{i}", idx)

    for i, col in enumerate(data_cols):
        ds.add_col(f"col-{i}", col)

    ctx.compile()

