from collections import namedtuple
from itertools import chain

from hypothesis import given
from hypothesis import strategies as st
from hypothesis.stateful import Bundle, RuleBasedStateMachine, rule
from pytest import raises

from lightcurvedb.managers import LightcurveManager
from lightcurvedb.managers.manager import (
    DuplicateEntryException,
    manager_factory,
)

from .factories import lightcurve
from .factories import lightcurve_kwargs as lc_kw_st

UNIQ_COL_EXAMPLES = [x for x in "abcdefghijklmnopqrstuvwxyz"]

DATA_COL_EXAMPLES = ["data_{}".format(x) for x in "abcdefghijklmnopqrstuvwxyz"]


@given(
    st.from_regex(r"^[a-z]+$", fullmatch=True),
    st.sets(st.sampled_from(UNIQ_COL_EXAMPLES), min_size=1),
    st.sets(st.sampled_from(DATA_COL_EXAMPLES)),
)
def test_manager_class_definition(model_name, uniq_cols, data_cols):
    model_name = "Model_{}".format(model_name)
    cols = list(chain(uniq_cols, data_cols))
    TestModel = namedtuple(model_name, cols)
    Manager = manager_factory(TestModel, *uniq_cols)

    assert Manager is not None


class LCManagerComparison(RuleBasedStateMachine):
    def __init__(self):
        super(LCManagerComparison, self).__init__()
        self.reference = dict()
        self.manager = LightcurveManager()
        self.uniq_cols = ("tic_id", "aperture_id", "lightcurve_type_id")

    lightcurves = Bundle("lightcurves")

    @rule(target=lightcurves, lc=lightcurve())
    def add_lightcurve(self, lc):
        key = self.manager.__get_key__(lc)

        if key in self.manager:
            with raises(DuplicateEntryException):
                self.manager.add_model(lc)
            return None

        return self.manager.add_model(lc)

    @rule(target=lightcurves, lc_kw=lc_kw_st())
    def add_lightcurve_by_kwargs(self, lc_kw):
        key = self.manager.__get_key_by_kw__(**lc_kw)

        if key in self.manager:
            with raises(DuplicateEntryException):
                self.manager.add_model_kw(**lc_kw)
            return None
        return self.manager.add_model_kw(**lc_kw)

    @rule(lc=lightcurves)
    def assert_in_lightcurve_manager(self, lc):
        if lc:
            assert lc in self.manager


TestLCManager = LCManagerComparison.TestCase
