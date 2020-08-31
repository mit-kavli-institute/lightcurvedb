from hypothesis import strategies as st, given
from hypothesis.stateful import Bundle, RuleBasedStateMachine, rule, consumes
from lightcurvedb.models import Lightcurve
from .factories import lightpoint, lightcurve
import numpy as np


ASSIGNABLE_ATTRS = {
    "barycentric_julian_date",
    "bjd",
    "values",
    "errors",
    "x_centroids",
    "y_centroids",
    "quality_flags",
}


assignable_attr = lambda: st.one_of(st.just(col) for col in ASSIGNABLE_ATTRS)


class CollectionComparison(RuleBasedStateMachine):
    @given(lightcurve())
    def __init__(self, lc):
        super(CollectionComparison, self).__init__()
        self.reference = dict()
        self.lightcurve = lc

    lightpoints = Bundle("lightpoints")

    @rule(target=lightpoints, lp=lightpoint())
    def add_lightpoint(self, lp):
        cadence = lp.cadence
        self.reference[cadence] = lp
        self.lightcurve.lightpoints.add(lp)

        return lp

    @rule(lp=consumes(lightpoints))
    def remove_lightpoint(self, lp):
        cadence = lp.cadence
        if cadence in self.reference:
            del self.reference[cadence]
            self.lightcurve.lightpoints.remove(lp)
        else:
            assert lp not in self.lightcurve.lightpoints

    @rule(lp=lightpoints)
    def assert_validity(self, lp):
        cadence = lp.cadence
        if cadence in self.reference:
            assert lp in self.lightcurve.lightpoints

            # Assert correct data in collection
            check = lp.data == self.reference[cadence].data
            assert check == (
                lp.data == self.lightcurve.lightpoints[cadence].data
            )
        else:
            assert lp not in self.lightcurve.lightpoints


@given(assignable_attr(), lightcurve(), st.lists(lightpoint()))
def test_scalar_assignment(attr, lc, lightpoints):
    lc.lightpoints.extend(lightpoints)
    value = 0.0
    setattr(lc, attr, value)

    new = getattr(lc, attr)
    assert all(value == new_val for new_val in new)


@given(assignable_attr(), lightcurve(), st.lists(lightpoint()))
def test_list_assignment(attr, lc, lightpoints):
    lc.lightpoints.extend(lightpoints)
    value = np.arange(len(lc))
    setattr(lc, attr, value)

    new = getattr(lc, attr)
    np.testing.assert_equal(value, new)


TestCollectionComparison = CollectionComparison.TestCase
