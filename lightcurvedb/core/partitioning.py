"""
This module describes partitioning of the lightcurve database.
"""
from sqlalchemy import DDL
from sqlalchemy.orm import aliased
from math import ceil
import re
from pandas import to_numeric


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
            "Cannot make a partition on {0} since the columns that "
            "are passed are empty".format(
                partition_type
            )
        )

    class __PartitionMeta__(object):
        __abstract__ = True
        __table_args__ = dict(
            postgresql_partition_by="{0}({1})".format(
                partition_type, ",".join(columns)
            ),
            extend_existing=True
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


def n_new_partitions(
    current_value, current_part_val, est_new_vals, partition_range
):
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


def emit_ranged_partition_ddl(table, begin_range, end_range, schema=None):
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
    schema : str, optional
        The schema to place the table under. None implies the use of the
        "public" schema.

    Returns
    -------
    sqlalchemy.DDL
    """

    namespaced_t = '{0}.{1}'.format(schema, table) if schema else table

    fmt_args = dict(
        partition=namespaced_t,
        table=table,
        begin=begin_range,
        end=end_range
        )

    return DDL(
        "CREATE TABLE {partition}_{begin}_{end} "
        "PARTITION OF {table} FOR VALUES FROM ({begin}) "
        "TO ({end})".format(
            **fmt_args
        )
    )


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
    regex = re.compile((
        r"^FOR VALUES FROM \('(?P<begin_range>\d+)'\) "
        r"TO \('(?P<end_range>\d+)'\)$"
    ))

    result = partition_df["partition_expression"].str.extract(regex)
    result[["begin_range", "end_range"]] = result[
        ["begin_range", "end_range"]
    ].apply(to_numeric, errors="coerce")
    return result


def inheritance_join(db, psql_meta, tablename, attributes):
    pg_inherits = psql_meta.tables['pg_catalog.pg_inherits']
    pg_class = psql_meta.tables['pg_catalog.pg_class']

    parent = aliased(pg_class, alias='parent')
    child = aliased(pg_class, alias='child')

    q = db.query(*attributes).join(
        pg_inherits,
        child.c.oid == pg_inherits.c.inhrelid
    ).join(
        parent,
        parent.c.oid == pg_inherits.c.inhparent
    ).filter(
            parent.c.relname == tablename
    )
    return q


def get_partition_tables(psql_meta, model, db, resolve=True):
    """
    Query for the partition tables of the given SQLAlchemy Model.
    This will return ``pg_class`` rows of partition tables that
    are partitioned on ``model.__tablename__``.

    Parameters
    ----------
    psql_meta : sqlalchemy.MetaData
        The metadata object that is a reflection of the POSTGRESQL
        catalogs.
    model : lightcurvedb.core.base_model.QLPModel
        The Model class to reference for partitions.
    db : lightcurvedb.db
        The current and open db class instance.

    Returns
    -------
    list
        Returns a list of tuples of the executed query. Potentially
        empty.
    """
    tablename = model.__tablename__
    pg_class = psql_meta.tables['pg_catalog.pg_class']

    child = aliased(pg_class, alias='child')

    q = inheritance_join(db, psql_meta, tablename, [child])

    return q.all() if resolve else q


def get_partition_columns(psql_meta, model, attrs, db, resolve=True):
    tablename = model.__tablename__
    pg_class = psql_meta.tables['pg_catalog.pg_class']

    child = aliased(pg_class, alias='child')

    columns = (getattr(child.c, column) for column in attrs)

    q = inheritance_join(db, psql_meta, tablename, columns)

    return q.all() if resolve else q


def get_partition_q(tablename):
    return None
