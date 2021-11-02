import traceback
from math import ceil

from click.testing import CliRunner
from hypothesis import assume, example, given, note
from hypothesis import strategies as st

from lightcurvedb.cli.base import lcdbcli
from lightcurvedb.core.admin import psql_tables
from lightcurvedb.core.partitioning import n_new_partitions
from lightcurvedb.models import Lightpoint
from lightcurvedb.models.lightpoint import LIGHTPOINT_PARTITION_RANGE

from .constants import CONFIG_PATH
from .fixtures import clear_all, db_conn  # noqa F401


@given(
    st.integers(min_value=1),
    st.integers(min_value=1),
    st.integers(min_value=1),
    st.integers(min_value=1),
)
@example(
    current_value=1, current_partition_max=68, est_new_values=67, blocksize=2
)
def test_ranged_partition_calculation(
    current_value, current_partition_max, est_new_values, blocksize
):
    """
    Test that we can calculate the correct number of new partitions
    required
    """
    assume(current_partition_max > current_value)
    overflow = (current_value + est_new_values + 1) - current_partition_max
    note("overflow: {}".format(overflow))
    required_partitions = overflow / blocksize
    note("raw required: {}".format(required_partitions))
    note("ceil requried: {}".format(ceil(required_partitions)))

    n_required_new_partitions = n_new_partitions(
        current_value, current_partition_max, est_new_values, blocksize
    )

    if current_value + est_new_values >= current_partition_max:
        assert n_required_new_partitions >= 1
    else:
        assert n_required_new_partitions == 0


@given(st.just(1))
def test_cli_creation_of_partition(db_conn, n_partitions):  # noqa F401
    with db_conn as db:
        orig_n_partitions = len(db.get_partitions_df(Lightpoint))
        runner = CliRunner(
            mix_stderr=False  # Dunno why this defaults to TRUE :(
        )
        result = runner.invoke(
            lcdbcli,
            [
                "--dbconf",
                CONFIG_PATH,
                "partitioning",
                "create-partitions",
                "Lightpoint",
                str(n_partitions),
                str(LIGHTPOINT_PARTITION_RANGE),
            ],
            input="yes",
        )
        df = db.get_partitions_df(Lightpoint)
        note(df)
        new_len = len(df)

        note(result.exit_code)
        note(result.output)
        note(result.stderr)
        note(traceback.format_list(traceback.extract_tb(result.exc_info[2])))

        assert not result.exception
        assert result.exit_code == 0
        assert new_len - orig_n_partitions == 1


def test_partitioning_classes(db_conn):  # noqa F401
    with db_conn as db:
        db.commit()
        psql_tables(db)
        assert db.query(Lightpoint.partition_oids).count() >= 1
