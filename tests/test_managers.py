from .factories import lightcurve_kwargs as lc_kw_st, lightcurve as lc_st
from itertools import chain
from collections import namedtuple
from hypothesis import strategies as st, given
from hypothesis.stateful import RuleBasedStateMachine, Bundle, rule
from lightcurvedb.managers.manager import manager_factory


UNIQ_COL_EXAMPLES = [
    x for x in 'abcdefghijklmnopqrstuvwxyz'
]

DATA_COL_EXAMPLES = [
    'data_{}'.format(x) for x in 'abcdefghijklmnopqrstuvwxyz'
]


@given(st.from_regex(r'^[a-z]+$', fullmatch=True), st.sets(st.sampled_from(UNIQ_COL_EXAMPLES), min_size=1), st.sets(st.sampled_from(DATA_COL_EXAMPLES)))
def test_manager_class_definition(model_name, uniq_cols, data_cols):
    model_name = 'Model_{}'.format(model_name)
    cols = list(chain(uniq_cols, data_cols))
    TestModel = namedtuple(model_name, cols)
    Manager = manager_factory(TestModel, *uniq_cols)

