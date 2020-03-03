from hypothesis import strategies as st
from hypothesis import given, note, assume, settings, HealthCheck
from lightcurvedb.models.lightcurve import Lightcurve, Lightpoint
from lightcurvedb.core.ingestors.lightpoint import get_cadence_info

from .fixtures import db_conn
from .factories import lightcurve as lightcurve_st

@settings(suppress_health_check=[HealthCheck.too_slow])
@given(st.lists(lightcurve_st(), min_size=1, max_size=10, unique_by=lambda l: l.id))
def test_cadence_annotation(db_conn, lightcurves):
    try:
        db_conn.session.begin_nested()
        db_conn.session.add_all(lightcurves)
        db_conn.commit()

        tics = {lc.tic_id for lc in lightcurves}
        reference = {lc.id: set(lc.cadences) for lc in lightcurves}
        mapping = db_conn.session.execute(get_cadence_info(tics))

        for mapped in mapping:
            note(mapped)
            lc_id, min_cadence, max_cadence = mapped
            ref_cadences = reference[lc_id]

            if min_cadence is None or max_cadence is None:
                assert len(ref_cadences) == 0
            else:
                assert min(ref_cadences) == min_cadence
                assert max(ref_cadences) == max_cadence

        db_conn.session.rollback()
    except:
        db_conn.session.rollback()
        raise
