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


@given(st.data())
@settings(deadline=None)
def test_update_multiple_lc(db_conn, data):
    with db_conn as db:

        aperture = data.draw(aperture_st())
        lightcurve_type = data.draw(lightcurve_type_st())
        lightcurves = data.draw(
            lightcurve_list(
                apertures=st.just(aperture),
                lightcurve_types=st.just(lightcurve_type)
            )
        )
        orbit = data.draw(orbit_st())

        try:
            common_cadences = np.unique(
                np.concatenate(
                    [lc.cadences for lc in lightcurves]
                )
            )
            new_qflags = np.ones(common_cadences.shape, dtype=int)

            ref_qflag = pd.DataFrame(
                index=common_cadences,
                data={'quality_flags': new_qflags}
            )

            db.session.rollback()
            db.session.add(orbit)

            for tic in set(lc.tic_id for lc in lightcurves):
                observation = Observation(
                    tic_id=tic,
                    camera=1,
                    ccd=1,
                    orbit=orbit
                )
                db.session.add(observation)

            import_lc_prereqs(db, lightcurves)
            db.commit()

            db.session.bulk_save_objects(
                lightcurves, return_defaults=True
            )
            db.commit()

            db.set_quality_flags(
                orbit.orbit_number,
                1,
                1,
                common_cadences,
                new_qflags
            )
            db.commit()

            for lc in lightcurves:
                note(lc.id)
                test_df = pd.read_sql(
                    db.query(
                        func.unnest(Lightcurve.cadences).label('cadences'),
                        func.unnest(Lightcurve.quality_flags).label('quality_flags')
                    ).filter(
                        Lightcurve.id == lc.id
                    ).statement,
                    db.session.bind,
                    index_col=['cadences']
                )
                note(test_df)
                note(ref_qflag)

                np.testing.assert_array_equal(
                    test_df['quality_flags'],
                    ref_qflag.loc[test_df.index]['quality_flags']
                )

        finally:
            db.session.rollback()
            clear_all(db)

