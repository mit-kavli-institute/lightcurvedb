import itertools
from tempfile import TemporaryDirectory

import numpy as np
from hypothesis import given
from hypothesis import strategies as st

from lightcurvedb.core.ingestors.lightcurves import h5_to_numpy

from .strategies import ingestion


@given(
    st.lists(st.text(min_size=1), min_size=1, max_size=5, unique=True),
    st.lists(st.text(min_size=1), min_size=1, max_size=5, unique=True),
    st.integers(),
    st.data(),
)
def test_h5_to_numpy(apertures, lightcurve_types, lightcurve_id, data):
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
            assert np.array_equal(check["cadence"], bg["cadence"].to_numpy())
            assert np.array_equal(
                check["barycentric_julian_date"],
                bg["barycentric_julian_date"].to_numpy(),
            )
            assert np.array_equal(check["data"], ref["data"].to_numpy())
            assert np.array_equal(check["error"], ref["error"].to_numpy())
            assert np.array_equal(
                check["x_centroid"], ref["x_centroid"].to_numpy()
            )
            assert np.array_equal(
                check["y_centroid"], ref["y_centroid"].to_numpy()
            )
