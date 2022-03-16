"""Definitions for easily creating Temporary Tables in SQLAlchemy.

SQLAlchemy provides Table APIs but this can be cumbersome when dealing
with tables with temporary lifespans. A lot of similar SQL emissions must
be made.
"""

from contextlib import ContextDecorator
from loguru import logger
from sqlalchemy import Table, Column, MetaData, exc as sqlexc, event, schema, types
from lightcurvedb.core.sql import _str_to_sql_type


_tempmeta = MetaData()


class TempTable(ContextDecorator):
    """
    A temporary SQLAlchemy Table which can be assigned columns before its
    database schema creation.

    Examples
    --------
    >>> table = TempTable(open_db_inst, "tablename")
    >>> table.add_column("col1", Integer, primary_key=True)
    >>> with table as t:
        t.insert(col1=1)
        t.insert(col2=2)
        print(open_db_inst.query(t.col2).all())
    """
    _db = None
    _table = None

    def __init__(self, db, name):
        """
        Initializes a blank temporary table. This table will be created using
        the prefix TEMPORARY.

        Parameters
        ----------
        db: lightcurvedb.core.connection.ORM_DB
            A database object. This object must have an active connection to
            the relevant database when entering the table context.
        name: str
            The name for this table.
        """
        self._db = db
        self._table = Table(name, _tempmeta, prefixes=["TEMPORARY"])

    def __getattr__(self, name):
        return getattr(self.table, name)

    def __getitem__(self, key):
        return self.table.c[key]

    def __enter__(self):
        """
        Creates the temporary table for the lifetime of the with or decorator
        block. This requires that the attached database instance have a
        healthy connection. This method will raise an error if an attempt to
        create a table without an open connection is made.
        """
        ddl = schema.CreateTable(self._table)
        self._db.execute(ddl)
        return self

    def __exit__(self, exc_type, exc, exc_info):
        """
        Clean up the resource by dropping the table. If an exception occured
        where the database transaction is invalid, a rollback will be issued.
        """
        if isinstance(exc, sqlexc.DBAPIError):
            logger.debug(
                "SQL Exception with temp table, "
                "rolling back database transaction"
            )
            self._db.rollback()
        return exc_type is None

    def add_column(self, name, type_, **kwargs):
        """
        Add a column definition to the temporary table.

        Parameters
        ----------
        name: str
            The name for the column.
        type_ : sqlalchemy.Type

        """
        if isinstance(type_, str):
            sql_type = _str_to_sql_type(type_)
        else:
            sql_type = type_
        column = Column(name, sql_type, **kwargs)
        self._table.append_column(column)

    def insert(self, **values):
        """
        Insert a row into the table.
        """
        stmt = self._table.insert().values(**values)
        return self._db.execute(stmt)

    def insert_many(self, values, scalar=False):
        """
        Bulk insert the given values.

        Parameters
        ----------
        values: iter
            An iterable of values to insert in one bulk operation.
        scalar: bool
            If true, wrap the values iterable with (val, ). This is for easy
            insertion of a temp table with a single column.
        """
        stmt = self._table.insert()
        if scalar:
            stmt = stmt.values(list((val,) for val in values))
        else:
            stmt = stmt.values(list(values))

        return self._db.execute(stmt)

    @property
    def table(self):
        return self._table
