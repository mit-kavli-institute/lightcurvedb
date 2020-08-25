from hypothesis import strategies as st, given, note
from lightcurvedb.models import Lightcurve, Lightpoint
from .factories import lightcurve_type, aperture
from .constants import PSQL_INT_MAX
import numpy as np


@given(st.integers(min_value=1), aperture(), lightcurve_type())
def test_lightcurve_instantiation(tic, aperture, lc_type):
    lc = Lightcurve(tic_id=tic, aperture=aperture, lightcurve_type=lc_type)
    assert lc.tic_id == tic
    assert lc.aperture == aperture
    assert lc.type == lc_type


@given(
    st.builds(
        Lightpoint,
        lightcurve_id=st.integers(min_value=1, max_value=99999),
        cadence=st.integers(min_value=0, max_value=PSQL_INT_MAX),
        bjd=st.floats(),
        data=st.floats(),
        error=st.floats(),
        x=st.floats(),
        y=st.floats(),
        quality_flag=st.integers(min_value=0, max_value=PSQL_INT_MAX)
    ),
    st.integers(min_value=1),
    aperture(),
    lightcurve_type())
def test_lightpoint_collection_append(lp, tic, aperture, lc_type):
    lc = Lightcurve(tic_id=tic, aperture=aperture, lightcurve_type=lc_type)

    assert len(lc) == 0

    lc.lightpoints.append(lp)

    assert len(lc) == 1
    assert lc.cadences[0] == lp.cadence

    if np.isnan(lp.bjd):
        assert np.isnan(lc.bjd[0])
    else:
        assert lc.bjd[0] == lp.bjd


t
