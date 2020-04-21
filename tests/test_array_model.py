from hypothesis import given, note, assume, settings, HealthCheck
from hypothesis.extra import numpy as np_st
from lightcurvedb import models
from lightcurvedb.util.merge import matrix_merge
from sqlalchemy import func, select
import numpy as np
import pandas as pd

from .fixtures import db_conn
from .factories import lightcurve as lightcurve_st

@settings(suppress_health_check=[HealthCheck.too_slow])
@given(lightcurve_st())
def test_instantiation(lightcurve):
    assert lightcurve is not None
    assert len(lightcurve) >= 0


@settings(suppress_health_check=[HealthCheck.too_slow])
@given(lightcurve_st(), lightcurve_st())
def test_merging_sorts(lc_1, lc_2):
    assume(len(lc_1) > 0 and len(lc_2) > 0)

    lc_1.merge(lc_2.to_df)

    note(lc_1.to_df)

    assert all(np.diff(lc_1.to_df.index) >= 0)


@settings(suppress_health_check=[HealthCheck.too_slow])
@given(lightcurve_st(), lightcurve_st())
def test_merging_unique(lc_1, lc_2):
    assume(len(lc_1) > 0 and len(lc_2) > 0)
    lc_1.merge(lc_2.to_df)

    note(lc_1.to_df)

    check = set(lc_1.to_df.index)
    assert len(lc_1) == len(check)


@settings(suppress_health_check=[HealthCheck.too_slow])
@given(lightcurve_st())
def test_lightcurve_add(db_conn, lightcurve):
    try:
        db_conn.session.begin_nested()
        db_conn.session.add(lightcurve)
        db_conn.session.commit()

        q = select([func.count()]).select_from(models.Lightcurve)
        result = db_conn.session.execute(q).scalar()
        assert result == 1
        db_conn.session.rollback()
    except:
        note(lightcurve)
        db_conn.session.rollback()
        raise


@settings(deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(lightcurve_st(), lightcurve_st())
def test_merging_priority(lc_1, lc_2):
    # Prioritize later arrays
    assume(len(lc_1) > 0 and len(lc_2) > 0)

    original_data = lc_1.to_df.copy()
    original_data.sort_index(inplace=True)
    merging_data = lc_2.to_df.copy()
    merging_data.sort_index(inplace=True)

    intersecting_indices = set(original_data.index).intersection(merging_data.index)

    lc_1.merge(merging_data)
    merged = lc_1.to_df

    raw_merged = pd.concat((original_data, merging_data))
    note(~raw_merged.index.duplicated(keep='last'))
    note(raw_merged[~raw_merged.index.duplicated(keep='last')])

    for index in intersecting_indices:

        merged_value = merged.loc[index]['values']
        merging_value = merging_data.loc[index]['values']

        if np.isnan(merged_value):
            assert np.isnan(merging_value)
        else:
            assert merged_value == merging_value



@settings(deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(lightcurve_st())
def test_datatypes(lightcurve):
    assume(len(lightcurve) > 0)
    df = lightcurve.to_df

    note(df)

    assert df.index.dtype == np.int64
    assert df['bjd'].dtype == np.float64
    assert df['values'].dtype == np.float64
    assert df['quality_flags'].dtype == np.int64
