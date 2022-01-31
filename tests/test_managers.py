from collections import namedtuple
from itertools import chain, permutations

from hypothesis import given
from hypothesis import strategies as st
from hypothesis.stateful import Bundle, RuleBasedStateMachine, rule
from pytest import raises

from lightcurvedb.managers import LightcurveManager
from lightcurvedb.managers.manager import (
    DuplicateEntryException,
    manager_factory,
)
from lightcurvedb.models import Lightcurve

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
        self.manager = LightcurveManager([])
        self.uniq_cols = ("tic_id", "aperture_id", "lightcurve_type_id")

    lightcurves = Bundle("lightcurves")

    @rule(target=lightcurves, lc=lightcurve())
    def add_lightcurve(self, lc):
        key = self.manager.__get_key__(lc)

        if key in self.manager:
            with raises(DuplicateEntryException):
                self.manager.add_model(lc)
            return self.manager._interior_data[key]

        return self.manager.add_model(lc)

    @rule(target=lightcurves, lc_kw=lc_kw_st())
    def add_lightcurve_by_kwargs(self, lc_kw):
        key = self.manager.__get_key_by_kw__(
            tic_id=lc_kw["tic_id"],
            aperture_id=lc_kw["aperture"].name,
            lightcurve_type_id=lc_kw["lightcurve_type"].name,
        )

        if key in self.manager:
            with raises(DuplicateEntryException):
                self.manager.add_model_kw(
                    tic_id=lc_kw["tic_id"],
                    aperture_id=lc_kw["aperture"].name,
                    lightcurve_type_id=lc_kw["lightcurve_type"].name,
                )
            return self.manager._interior_data[key]

        return self.manager.add_model_kw(
            tic_id=lc_kw["tic_id"],
            aperture_id=lc_kw["aperture"].name,
            lightcurve_type_id=lc_kw["lightcurve_type"].name,
        )

    @rule(lc=lightcurves)
    def assert_in_lightcurve_manager(self, lc):
        if lc:
            assert lc in self.manager

    @rule(lc=lightcurves)
    def grab_from_manager(self, lc):
        keys = (lc.tic_id, lc.aperture_id, lc.lightcurve_type_id)
        for key in permutations(keys):
            check = self.manager
            for scalar_key in key:
                check = check[scalar_key]
                if isinstance(check, Lightcurve):
                    assert check == lc
                    break


TestLCManager = LCManagerComparison.TestCase
