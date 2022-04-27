import itertools
from tempfile import TemporaryDirectory

import numpy as np
from hypothesis import HealthCheck, assume, given, settings
from hypothesis import strategies as st

from lightcurvedb import models
from lightcurvedb.cli import lcdbcli
from lightcurvedb.core.ingestors.lightcurves import h5_to_numpy

from .strategies import ingestion
from .strategies import orm as orm_st

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


@settings(deadline=None, suppress_health_check=(HealthCheck.too_slow,))
@given(st.data())
def test_h5_ingestion(clirunner, data):
    database = data.draw(orm_st.database())
    try:
        with TemporaryDirectory() as tempdir, database as db:
            ingestion.simulate_lightcurve_ingestion_environment(
                data, tempdir, db
            )
            clirunner.invoke(
                lcdbcli,
                ["--dbconf", db._config, "lightcurve", "ingest-dir", tempdir],
                catch_exceptions=False,
            )
    finally:
        with database as db:
            opt = {"synchronize_session": False}
            db.query(models.Lightpoint).delete(**opt)
            db.query(models.Lightcurve).delete(**opt)
            db.query(models.Observation).delete(**opt)
            db.query(models.Frame).delete(**opt)
            db.query(models.FrameType).delete(**opt)
            db.query(models.LightcurveType).delete(**opt)
            db.query(models.Aperture).delete(**opt)
            db.query(models.Orbit).delete(**opt)
            db.query(models.SpacecraftEphemris).delete(**opt)
            db.commit()
