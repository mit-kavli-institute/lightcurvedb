from hypothesis import strategies as st, given, note
from lightcurvedb import Lightcurve, __version__
from lightcurvedb.models.metrics import QLPProcess, QLPAlteration
from packaging.version import Version, parse


@given(st.integers(), st.integers(), st.datetimes(), st.datetimes())
def test_load_module(n_rows, row_size, start, end):
    qa = QLPAlteration(
        process_id=None,
        target_model="lightcurvedb.models.lightcurve.Lightcurve",
        alteration_type="insert",
        n_altered_items=n_rows,
        est_item_size=row_size,
        time_start=start,
        time_end=end,
    )

    Class = qa.model
    assert Class == Lightcurve


@given(
    st.text(min_size=1, max_size=255),
    st.integers(min_value=0, max_value=32767),
    st.integers(min_value=0, max_value=32767),
    st.integers(min_value=0, max_value=32767),
)
def test_process_version(job_type, major, minor, revision):
    process = QLPProcess(job_type=job_type)
    process.version = "{}.{}.{}".format(major, minor, revision)

    assert process.job_version_major == major
    assert process.job_version_minor == minor
    assert process.job_version_revision == revision


@given(
    st.text(min_size=1, max_size=255),
    st.integers(min_value=0, max_value=32767),
    st.integers(min_value=0, max_value=32767),
    st.integers(min_value=0, max_value=32767),
)
def test_process_jsonb(job_type, major, minor, revision):
    process = QLPProcess(job_type=job_type)
    process.version = "{}.{}.{}".format(major, minor, revision)

    note(process.additional_version_info)
    assert process.additional_version_info["major"] == major
    assert process.additional_version_info["minor"] == minor


def test_load_lightcurvedb_version():
    instance = QLPProcess.lightcurvedb_process("test")
    ref_version = parse(__version__)

    assert instance.version == ref_version
