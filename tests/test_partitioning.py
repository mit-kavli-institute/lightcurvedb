from hypothesis import strategies as st, given, note, example, assume
from lightcurvedb.models.lightcurve import Lightpoint, LIGHTPOINT_PARTITION_RANGE
from lightcurvedb.core.partitioning import n_new_partitions, get_partition_q
from math import ceil
import traceback
from click.testing import CliRunner
from lightcurvedb.cli.base import lcdbcli
from .fixtures import db_conn, clear_all
from .constants import CONFIG_PATH


@given(st.integers(min_value=1), st.integers(min_value=1), st.integers(min_value=1), st.integers(min_value=1))
@example(current_value=1, current_partition_max=68, est_new_values=67, blocksize=2)
def test_ranged_partition_calculation(current_value, current_partition_max, est_new_values, blocksize):
    """
    Test that we can calculate the correct number of new partitions
    required
    """
    assume(current_partition_max > current_value)
    overflow = (current_value + est_new_values + 1) - current_partition_max
    note(
        'overflow: {}'.format(
            overflow
        )
    )
    required_partitions = overflow / blocksize
    note('raw required: {}'.format(required_partitions))
    note('ceil requried: {}'.format(ceil(required_partitions)))


    n_required_new_partitions = n_new_partitions(
        current_value, current_partition_max, est_new_values, blocksize
    )

    if current_value + est_new_values >= current_partition_max:
        assert n_required_new_partitions >= 1
    else:
        assert n_required_new_partitions == 0


def test_partition_info_q(db_conn):
    """
    Test that we can query for partition info. Assumes that the initial
    partition for lightpoints has been made.
    """
    with db_conn as db:
        proxy = db.session.execute(
            get_partition_q(
                Lightpoint.__tablename__
            )
        )

        cur_bound = 0
        for partition_name, bound_str in proxy:
            assert 'lightpoints' in partition_name
            expected_bound_str = "FOR VALUES FROM ('{}') TO ('{}')".format(
                cur_bound,
                cur_bound + LIGHTPOINT_PARTITION_RANGE,
            )
            assert bound_str == expected_bound_str
            cur_bound += LIGHTPOINT_PARTITION_RANGE

        db.rollback()
        clear_all(db)


@given(st.just(1))
def test_cli_creation_of_partition(db_conn, n_partitions):
    with db_conn as db:
        orig_n_partitions = len(db.get_partitions_df(Lightpoint))
        runner = CliRunner(
            mix_stderr=False  # Dunno why this defaults to TRUE :(
        )
        result = runner.invoke(
            lcdbcli,
            [
                '--dbconf', CONFIG_PATH,
                '--scratch', '.',
                '--qlp-data', '.',
                'partitioning',
                'create-partitions',
                'Lightpoint', str(n_partitions), str(LIGHTPOINT_PARTITION_RANGE)
            ],
            input='yes'
        )
        df = db.get_partitions_df(Lightpoint)
        note(df)
        new_len = len(df)

        note(result.exit_code)
        note(result.output)
        note(result.stderr)
        note(
            traceback.format_list(
                traceback.extract_tb(result.exc_info[2])
            )
        )

        assert not result.exception
        assert result.exit_code == 0
        assert new_len - orig_n_partitions == 1
