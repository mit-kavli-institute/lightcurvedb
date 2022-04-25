import itertools
from tempfile import TemporaryDirectory

import numpy as np
from hypothesis import assume, given
from hypothesis import strategies as st

from lightcurvedb.core.ingestors.lightcurves import h5_to_numpy

from .strategies import ingestion

FORBIDDEN_KEYWORDS = (
    "\x00",
    "X",
    "Y",
    "Cadence",
    "BJD",
    "QualityFlag",
    "LightCurve",
    "AperturePhotometry",
)


@given(
    st.lists(st.text(min_size=1), min_size=1, max_size=5, unique=True),
    st.lists(st.text(min_size=1), min_size=1, max_size=5, unique=True),
    st.integers(min_value=0, max_value=2 ** 64),
    st.data(),
)
def test_h5_to_numpy(apertures, lightcurve_types, lightcurve_id, data):
    assume(
        all(
            all(keyword not in name for keyword in FORBIDDEN_KEYWORDS)
            for name in itertools.chain(apertures, lightcurve_types)
        )
    )
    with TemporaryDirectory() as tempdir:
        h5_path, data_gen_ref = ingestion.simulate_h5_file(
            data,
            tempdir,
            "BackgroundAperture",
            "Background",
            apertures,
            lightcurve_types,
        )
        bg = data_gen_ref["background"]
        for ap, lc_t in itertools.product(apertures, lightcurve_types):
            check = h5_to_numpy(lightcurve_id, ap, lc_t, h5_path)
            ref = data_gen_ref["photometry"][(ap, lc_t)]
            np.testing.assert_array_equal(check["cadence"], bg["cadence"][0])
            np.testing.assert_array_equal(
                check["barycentric_julian_date"],
                bg["barycentric_julian_date"][0],
            )
            np.testing.assert_array_equal(check["data"], ref["data"][0])
            np.testing.assert_array_equal(check["error"], ref["error"][0])
