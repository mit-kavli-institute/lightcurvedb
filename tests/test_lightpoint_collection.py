from hypothesis import strategies as st, given
from hypothesis.stateful import Bundle, RuleBasedStateMachine, rule, consumes
from lightcurvedb.models import Lightcurve
from .factories import lightpoint, lightcurve

class CollectionComparison(RuleBasedStateMachine):

    @given(lightcurve())
    def __init__(self, lc):
        super(CollectionComparison, self).__init__()
        self.reference = dict()
        self.lightcurve = lc

    lightpoints = Bundle('lightpoints')

    @rule(target=lightpoints, lp=lightpoint())
    def add_lightpoint(self, lp):
        cadence = lp.cadence
        self.reference[cadence] = lp
        self.lightcurve.lightpoints.add(lp)

        return lp

    @rule(lp=consumes(lightpoints))
    def remove_lightpoint(self, lp):
        cadence = lp.cadence
        self.reference[cadence]
        self.lightcurve.lightpoints.remove(lp)

    @rule(lp=lightpoints)
    def assert_validity(self, lp):
        cadence = lp.cadence
        if cadence in self.reference:
            assert lp in self.lightcurve.lightpoints

            # Assert correct data in collection
            check = lp.data == self.reference[cadence].data
            assert check == (lp.data == self.lightcurve.lightpoints[cadence].data)
        else:
            assert lp not in self.lightcurve.lightpoints


TestCollectionComparison = CollectionComparison.TestCase
