from hypothesis import given, note, assume
from lightcurvedb import models

from .fixtures import db_conn
from .factories import array_lightcurve as lightcurve_st

@given(lightcurve_st())
def test_instantiation(lightcurve):
    assert lightcurve is not None
    assert len(lightcurve) >= 0
