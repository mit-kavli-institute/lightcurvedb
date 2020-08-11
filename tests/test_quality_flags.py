import numpy as np
import pandas as pd
from hypothesis import given, note, settings
from hypothesis import strategies as st
from hypothesis.extra import numpy as np_t

from lightcurvedb.core.base_model import QLPModel
from lightcurvedb.models import Lightcurve, Observation, Orbit
from lightcurvedb.core.quality_flags import set_quality_flags
from sqlalchemy import func

from .factories import aperture as aperture_st
from .factories import lightcurve as lightcurve_st
from .factories import lightcurve_list
from .factories import lightcurve_type as lightcurve_type_st
from .factories import orbit as orbit_st
from .fixtures import clear_all, db_conn
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


@given(
    lightcurve_st(tic_id=st.just(1)),
    lightcurve_st(tic_id=st.just(2)),
    orbit_st(orbit_number=st.just(1)))
@settings(deadline=None)
def test_update_multiple_lc(db_conn, lc1, lc2, orbit):
    with db_conn as db:
        try:
            cadences = sorted(set(lc1.cadences) | set(lc2.cadences))
            quality_flags = np.ones(len(cadences))
            lc2.lightcurve_type = lc1.lightcurve_type
            lc2.aperture = lc1.aperture
            db.add(lc1)
            db.add(lc2)
            db.commit()

            set_quality_flags(
                db.session,
                db.query(Lightcurve.id),
                cadences,
                quality_flags
            )

            for lc in db.lightcurves.all():
                assert all(lc.quality_flags == 1)

        finally:
            # Cleanup
            db.session.rollback()
            clear_all(db)
