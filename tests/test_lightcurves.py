from hypothesis import given
from numpy import typing as npt

from lightcurvedb.models.lightcurve import ArrayOrbitLightcurve

from .strategies import ingestion

FORBIDDEN_KEYWORDS = (
    "\x00",
    "/",
    "X",
    "Y",
    ".",
    "Cadence",
    "BJD",
    "QualityFlag",
    "LightCurve",
    "AperturePhotometry",
)


@given(ingestion.cadences())
def test_lightcurve_length(cadences: npt.NDArray):
    lc = ArrayOrbitLightcurve(cadences=cadences)
    assert len(lc) == cadences.shape[0]
