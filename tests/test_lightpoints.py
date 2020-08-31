from lightcurvedb.models import Lightpoint
from lightcurvedb.core.ingestors.lightpoint import lightpoint_upsert_q
from hypothesis import strategies as st, given, settings
from .fixtures import db_conn, clear_all
from .factories import lightcurve as lc_st
from .constants import PSQL_INT_MAX


@settings(deadline=None)
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
        quality_flag=st.integers(min_value=0, max_value=PSQL_INT_MAX),
    ),
    lc_st(),
)
def test_lightpoint_retrieval(db_conn, lp, lc):
    with db_conn as db:
        try:
            lc.id = lp.lightcurve_id
            db.add(lc)
            db.add(lp)
            db.commit()

            result = db.query(Lightpoint).get((lp.lightcurve_id, lp.cadence))

            assert result == lp
            assert result.lightcurve == lc

        finally:
            db.rollback()
            clear_all(db)


# @settings(deadline=None)
# @given(
#     st.builds(
#         Lightpoint,
#         lightcurve_id=st.integers(min_value=1, max_value=99999),
#         cadence=st.integers(min_value=0, max_value=PSQL_INT_MAX),
#         bjd=st.floats(),
#         data=st.floats(),
#         error=st.floats(),
#         x=st.floats(),
#         y=st.floats(),
#         quality_flag=st.integers(min_value=0, max_value=PSQL_INT_MAX)
#     ),
#     lc_st()
# )
# def test_lightpoint_upsert_tautological(db_conn, lp, lc):
#     with db_conn as db:
#         try:
#             lc.id = lp.lightcurve_id
#             db.add(lc)
#             db.commit()
#
#             assert len(lc) == 0
#
#             q = lightpoint_upsert_q()
#             db.session.execute(
#                 q, [
#                     {
#                         'lightcurve_id': lp.lightcurve_id,
#                         'cadence': lp.cadence,
#                         'barycentric_julian_date': lp.barycentric_julian_date,
#                         'data': lp.data,
#                         'error': lp.error,
#                         'x_centroid': lp.x_centroid,
#                         'y_centroid': lp.y_centroid,
#                         'quality_flag': lp.quality_flag
#                     }
#                 ]
#             )
#             db.commit()
#
#             assert len(lc) == 1
#         finally:
#             db.rollback()
#             clear_all(db)
#
#
# @settings(deadline=None)
# @given(
#     st.builds(
#         Lightpoint,
#         lightcurve_id=st.integers(min_value=1, max_value=99999),
#         cadence=st.integers(min_value=0, max_value=PSQL_INT_MAX),
#         bjd=st.floats(),
#         data=st.floats(min_value=10.0),
#         error=st.floats(),
#         x=st.floats(),
#         y=st.floats(),
#         quality_flag=st.integers(min_value=0, max_value=PSQL_INT_MAX)
#     ),
#     lc_st()
# )
# def test_lightpoint_upsert_update(db_conn, lp, lc):
#     with db_conn as db:
#         try:
#             lc.id = lp.lightcurve_id,
#             db.add(lc)
#             db.add(lp)
#             db.commit()
#             kw = lp.to_dict
#             kw['data'] = -1.0
#
#             q = lightpoint_upsert_q()
#             db.session.execute(
#                 q,
#                 [kw]
#             )
#             db.commit()
#             assert lc.values[0] == kw['data']
#         finally:
#             db.rollback()
#             clear_all(db)
#
# @settings(deadline=None)
# @given(
#     st.builds(
#         Lightpoint,
#         lightcurve_id=st.integers(min_value=1, max_value=99999),
#         cadence=st.integers(min_value=0, max_value=PSQL_INT_MAX),
#         bjd=st.floats(),
#         data=st.floats(min_value=10.0),
#         error=st.floats(),
#         x=st.floats(),
#         y=st.floats(),
#         quality_flag=st.integers(min_value=0, max_value=PSQL_INT_MAX)
#     ),
#     lc_st()
# )
# def test_lightpoint_upsert_nothing(db_conn, lp, lc):
#     with db_conn as db:
#         try:
#             lc.id = lp.lightcurve_id,
#             db.add(lc)
#             db.add(lp)
#             db.commit()
#             kw = lp.to_dict
#             kw['data'] = -1.0
#
#             q = lightpoint_upsert_q(mode='nothing')
#             db.session.execute(
#                 q,
#                 [kw]
#             )
#             db.commit()
#             assert lc.values[0] == lp.data
#         finally:
#             db.rollback()
#             clear_all(db)
