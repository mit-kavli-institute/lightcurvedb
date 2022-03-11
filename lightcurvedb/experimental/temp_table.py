"""Definitions for easily creating Temporary Tables in SQLAlchemy.

SQLAlchemy provides Table APIs but this can be cumbersome when dealing
with tables with temporary lifespans. A lot of similar SQL emissions must
be made.
"""

from contextlib import ContextDecorator
from loguru import logger
from sqlalchemy import Table, Column, orm, exc as sqlexc


TempBase = orm.declarative_base()


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
    _columns = {}
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
        self._table = Table(name, TempBase, prefixes=["TEMPORARY"])

    def __enter__(self):
        """
        Creates the temporary table for the lifetime of the with or decorator
        block. This requires that the attached database instance have a
        healthy connection. This method will raise an error if an attempt to
        create a table without an open connection is made.
        """
        engine = self._db.session.engine
        self._table.create(bind=engine)
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
        self._table.drop()

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
        column = Column(name, type_, **kwargs)
        self._table.append_column(column)

    def insert(self, **values):
        """
        Insert a row into the table.
        """
        stmt = self._table.insert().values(**values)
        return self._db.execute(stmt)
