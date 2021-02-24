"""
This module describes partitioning of the lightcurve database.
"""
from sqlalchemy import DDL, select, func
from sqlalchemy.orm import aliased
from sqlalchemy.ext.hybrid import hybrid_property
from math import ceil
import re
from pandas import to_numeric, read_sql as pd_read_sql
from lightcurvedb.core.admin import get_psql_catalog_tables


partition_range_extr = re.compile(
    (
        r"^FOR VALUES FROM \('(?P<begin_range>\d+)'\) "
        r"TO \('(?P<end_range>\d+)'\)$"
    )
)


def Partitionable(partition_type, *columns):
    """
    Create a mixin class specifying the table arguments that should
    be passed as an inheritance to another class declaration.

    Parameters
    ----------
    partition_type: str
        The partition type, for PSQL 12 this can be ``range``, ``list``,
        ``hash``, and some others. Please reference the postgresql
        partitioning documentation.
    *columns : Variable str parameter
        The columns to set as the partition type. Cannot be empty
    """

    if len(columns) == 0:
        raise ValueError(
            "Cannot make a partition on {0} since the columns that "
            "are passed are empty".format(partition_type)
        )

    class __PartitionMeta__(object):
        __abstract__ = True
        __table_args__ = {
            "postgresql_partition_by": "{0}({1})".format(
                partition_type, ",".join(columns)
            ),
            "extend_existing": True,
        }
        __partitioning_type__ = partition_type.lower()

        @classmethod
        def emit_new_partition(cls, table_identifier, constraint_str):
            """
            Tell SQLAlchemy DDL to create a new partition. Emits an
            SQLAlchemy DDL object to be executed. No validation is made
            to ensure that the given partition rule is correct or in agreement
            to other partitions.

            Parameters
            ----------
            table_identifier : str
                The unique suffix to have the partition tablename be under.
            constraint_str : str
                The constraint rule to apply

            """
            PARENT_TBL = cls.__tablename__
            PATTERN = "CREATE TABLE {0}_{{0}} PARTITION OF {0} FOR VALUES IN ({{1}})".format(
                PARENT_TBL
            )

            ddl_str = PATTERN.format(table_identifier, constraint_str)
            return DDL(ddl_str)

        @hybrid_property
        def partition_oids(self):
            pg_class, pg_inherits = get_psql_catalog_tables(
                "pg_class", "pg_inherits"
            )

        @partition_oids.expression
        def partition_oids(cls):
            pg_inherits = get_psql_catalog_tables("pg_inherits")

            return (
                select([pg_inherits.c.inhrelid])
                .where(pg_inherits.c.inhparent == cls.oid)
                .label("partition_oids")
            )

        @hybrid_property
        def partition_info(self):
            raise NotImplementedError

        @partition_info.expression
        def partition_info(cls):
            pg_class = get_psql_catalog_tables("pg_class")
            return select(
                [
                    pg_class.c.relname,
                    func.pg_get_expr(
                        pg_class.c.relpartbound, pg_class.c.oid
                    ).label("expression"),
                ]
            ).where(pg_class.c.oid.in_(cls.partition_oids))

        @classmethod
        def partition_df(cls, db):
            pg_class, pg_inherits = get_psql_catalog_tables(
                "pg_class", "pg_inherits"
            )

            child = aliased(pg_class, alias="child")
            parent = aliased(pg_class, alias="parent")

            info_q = (
                db.query(
                    child.c.relname,
                    func.pg_get_expr(child.c.relpartbound, child.c.oid).label(
                        "expression"
                    ),
                )
                .join(pg_inherits, child.c.oid == pg_inherits.c.inhrelid)
                .join(parent, parent.c.oid == pg_inherits.c.inhparent)
                .filter(parent.c.relname == cls.__tablename__)
            )

            df = pd_read_sql(info_q.statement, db.session.bind)

            result = df["expression"].str.extract(partition_range_extr)
            result[["begin_range", "end_range"]] = result[
                ["begin_range", "end_range"]
            ].apply(to_numeric, errors="coerce")
            return result

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

    namespaced_t = "{0}.{1}".format(schema, table) if schema else table

    fmt_args = {
        "partition": namespaced_t,
        "table": table,
        "begin": begin_range,
        "end": end_range,
    }

    return DDL(
        "CREATE TABLE {partition}_{begin}_{end} "
        "PARTITION OF {table} FOR VALUES FROM ({begin}) "
        "TO ({end})".format(**fmt_args)
    )
