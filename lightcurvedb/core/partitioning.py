"""
This module describes partitioning of the lightcurve database.
"""
from sqlalchemy import DDL, text
from math import ceil
import re


def Partitionable(partition_type, *columns):
    """
    Create a mixin class specifying the table arguments that should
    be passed as an inheritance to another class declaration.

    Parameters
    ----------
    partition_type: str
        The partition type, for PSQL 12 this can be ``range``, ``values``,
        ``hash``, and some others. Please reference the postgresql
        partitioning documentation.
    *columns : Variable str parameter
        The columns to set as the partition type. Cannot be empty
    """

    if len(columns) == 0:
        raise ValueError(
            'Cannot make a partition on {} since the columns that are passed are empty'.format(partition_type)
        )

    class __PartitionMeta__(object):
        __abstract__ = True
        __table_args__ = dict(
            postgresql_partition_by="{}({})".format(
                partition_type, ','.join(columns)
            )
        )

        def emit_new_partition(self, constraint_str):
            """
            Tell SQLAlchemy DDL to create a new partition. Emits an
            SQLAlchemy DDL object to be executed. No validation is made
            to ensure that the given partition rule is correct or in agreement
            to other partitions.

            Parameters
            ----------
            constraint_str : str
                The constraint rule to apply

            """
            raise NotImplementedError
           

    return __PartitionMeta__


def n_new_partitions(current_value, current_part_val, est_new_vals, partition_range):
    """
    Calculate the number of new partitions that would be needed for the
    given ``est_new_vals``.

    Parameters
    ----------
    current_value : numerical
        The current value that is closest to 'overflowing' the range of
        the latest partition.
    current_part_val : numerical
        The current end range (exclusive) of the partitions.
    est_new_vals : int
        The estimated number of new values.
    partition_range : numerical
        The allowed range that is applied to each partition.

    Returns
    -------
    int
        The number of new partitions needed to accomodate ``est_new_values``
    """

    overflow = (current_value + est_new_vals + 1) - current_part_val

    required_partitions = int(ceil(float(overflow) / float(partition_range)))
    if required_partitions < 0:
        required_partitions = 0

    return required_partitions


def emit_ranged_partition_ddl(table, begin_range, end_range):
    """
    Construct a DDL object representing the creation of a partition.

    Parameters
    ----------
    table : str
        The tablename to target for partitioning. This parameter is also
        used for determining ``tablespace`` parameters in the format of
        `tablespace`_partitions.

    begin_range : int or float
        The beginning range of the table. Used in naming and setting the
        constraints of the table.
    end_range : int or float
        The exclusive end of the range for the table. Used in naming and
        setting the constraints of the table.

    Returns
    -------
    sqlalchemy.DDL
    """

    fmt_args = dict(
        table=table,
        begin=begin_range,
        end=end_range,
    )

    return DDL(
        'CREATE TABLE {table}_{begin}_{end} PARTITION OF {table} FOR VALUES FROM ({begin}) TO ({end})'.format(
            **fmt_args
        )
    )


def get_partition_q(table):
    """
    Create a query searching for partitions of the given table.

    Parameters
    ----------
    table : str or sqlalchemy.Table

    Returns
    -------
    sqlalchemy.text
        A text object representing the desired query.
    """
    q = text(
            "SELECT pt.relname AS partition_name, pg_get_expr(pt.relpartbound, pt.oid, true) AS partition_expression FROM pg_class base_tb JOIN pg_inherits i ON i.inhparent = base_tb.oid JOIN pg_class pt ON pt.oid = i.inhrelid WHERE base_tb.oid = :t\:\:regclass"
    ).bindparams(t=table)
    return q


def extract_partition_df(partition_df):
    """
    Expands the ``partition_expression`` on a partition dataframe into
    ``begin_range`` and ``end_range``.

    Parameters
    ----------
    partition_df : pd.DataFrame
        The partition to extract on

    Returns
    -------
    pd.DataFrame
        The columns extracted via regex capture groups.
    """
    regex = re.compile(
        r'^FOR VALUES FROM \((?P<begin_range>\d+)\) TO \((?P<end_range>\d+)\)$'
    )

    return df['partition_expression'].extract(regex)
