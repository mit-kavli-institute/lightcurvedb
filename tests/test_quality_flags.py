import numpy as np
import pandas as pd
from sqlalchemy import func
from lightcurvedb.models import Lightcurve, Observation, Orbit
from lightcurvedb.core.base_model import QLPModel
from hypothesis import strategies as st, given, note, settings
from hypothesis.extra import numpy as np_t
from .factories import lightcurve as lightcurve_st, orbit as orbit_st, lightcurve_list, aperture as aperture_st, lightcurve_type as lightcurve_type_st
from .fixtures import db_conn, clear_all
from .util import import_lc_prereqs


@given(lightcurve_st(), orbit_st())
@settings(deadline=None)
def test_update_single_lc(db_conn, lc, orbit):
    # Ensure previous tests are in an OK state
    with db_conn as db:
        try:
            new_qflags = np.ones(len(lc), dtype=int)
            db.session.rollback()

            observation = Observation(
                tic_id=lc.tic_id,
                camera=1,
                ccd=1,
                orbit=orbit
            )
            db.add(orbit)
            db.add(lc)
            db.add(observation)
            db.commit()

            db.set_quality_flags(
                orbit.orbit_number,
                1,
                1,
                lc.cadences,
                new_qflags
            )
            np.testing.assert_array_equal(
                db.query(Lightcurve.quality_flags).filter(
                    Lightcurve.id == lc.id
                ).one()[0],
                new_qflags
            )
        finally:
            # Cleanup
            db.session.rollback()
            clear_all(db)
