from math import isnan

from hypothesis import given
from hypothesis import strategies as st

from lightcurvedb.cli.utils import slow_typecheck


@given(
    st.one_of(
        st.integers(),
        st.floats(),
        st.text(),
    )
)
def test_slow_typecheck(value):
    if isinstance(value, str):
        value = "'{0}'".format(value)

    cli_equivalent = str(value)

    if isinstance(value, float) and isnan(value):
        assert isnan(slow_typecheck(cli_equivalent))
    else:
        assert value == slow_typecheck(cli_equivalent)
