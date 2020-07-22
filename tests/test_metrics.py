from hypothesis import strategies as st, given, note
from lightcurvedb import Lightcurve
from lightcurvedb.models.metrics import QLPAlteration


@given(st.integers(), st.integers(), st.datetimes(), st.datetimes())
def test_load_module(n_rows, row_size, start, end):
    qa = QLPAlteration(
        process_id=None,
        target_model='lightcurvedb.models.Lightcurve',
        alteration_type='insert',
        n_altered_rows=n_rows,
        est_row_size=row_size,
        time_start=start,
        time_end=end
    )

    Class = qa.model
    assert Class == Lightcurve
