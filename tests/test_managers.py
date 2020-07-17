from .factories import lightcurve_kwargs as lc_kw_st, lightcurve as lc_st
from hypothesis.stateful import RuleBasedStateMachine, Bundle, rule
from lightcurvedb import LightcurveManager


class ManagerComparison(RuleBasedStateMachine):
    lightcurves = Bundle('lightcurves')
    lightcurve_kwargs = Bundle('kwargs')

    def __init__(self):
        super(ManagerComparison, self).__init__()
        self.lm = LightcurveManager

    @rule(target=lightcurves, lc=lc_st())
    def add_defined_lightcurve(self, lc):
        return lc

    @rule(target=lightcurve_kwargs, kw=lc_kw_st())
    def add_lightcurve_kwargs(self, kw):
        return kw

    @rule(lc=lightcurves)
    def add_defined_lc_to_manager(self, lc):
        self.lm.add_defined_lightcurve(lc)

    @rule(kw=lightcurve_kwargs)
    def upsert_kwargs(self, kw):
        self.lm.upsert(
            kw['tic_id'],
            kw['aperture'].name,
            kw['lightcurve_type'].name
        )

    @rule(lc=lightcurves, kw=lightcurve_kwargs)
    def check_merging(self, lc, kw):
        pass
