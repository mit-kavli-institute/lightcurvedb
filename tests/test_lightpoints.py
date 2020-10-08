from lightcurvedb.models import Lightpoint
from hypothesis import strategies as st, given, settings
from .fixtures import db_conn, clear_all
from .factories import lightcurve as lc_st
from .constants import PSQL_INT_MAX


# Expensive test, uncomment when need to run _full_ test.
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
#         quality_flag=st.integers(min_value=0, max_value=PSQL_INT_MAX),
#     ),
#     lc_st(),
# )
# def test_lightpoint_retrieval(db_conn, lp, lc):
#     with db_conn as db:
#         try:
#             lc.id = lp.lightcurve_id
#             db.add(lc)
#             db.add(lp)
#             db.commit()
# 
#             result = db.query(Lightpoint).get((lp.lightcurve_id, lp.cadence))
# 
#             assert result == lp
#             assert result.lightcurve == lc
# 
#         finally:
#             db.rollback()
#             clear_all(db)
