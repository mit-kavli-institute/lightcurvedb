"""
This module re-defines internal PostgreSQL catalog tables for fast
reflection.

This table should be modified depending on which version of PostgreSQL
is used.
"""

from sqlalchemy import Column, Integer, String, Text, DateTime, inspect
from sqlalchemy.dialects.postgresql import OID, INET
from sqlalchemy.ext.declarative import as_declarative
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.sql import func
from datetime import datetime


# Alias referenced datatypes to better match internal datatypes
XID = Integer

@as_declarative()
class PGStatModel(object):
    """
    Wrapper for PG Statistical Catalogs
    """

    __abstract__ = True


class PGStatActivity(PGStatModel):
    """
    Wraps behavior around PostgreSQL's pg_stat_activity.
    """
    __tablename__ = "pg_stat_activity"

    pid = Column(Integer, primary_key=True)

    datid = Column(OID)
    datname = Column(String(64))
    usesysid = Column(OID)
    usename = Column(String(64))
    application_name = Column(Text)

    client_addr = Column(INET)
    client_hostname = Column(Text)
    client_port = Column(Integer)

    backend_start = Column(DateTime(timezone=True))
    xact_start = Column(DateTime(timezone=True))
    query_start = Column(DateTime(timezone=True))
    state_change = Column(DateTime(timezone=True))

    wait_event_type = Column(String(64))
    wait_event = Column(String(64))

    state = Column(String(64))

    backend_xid = Column(XID)
    backend_xmin = Column(XID)

    query = Column(Text)
    backend_type = Column(Text)

    @hybrid_property
    def database(self):
        """
        Alias to the query's database name.

        Returns
        -------
        str:
            The database name.
        """
        return self.datname

    @database.expression
    def database(cls):
        """
        SQL Wrapper for the database hybrid property.
        """
        return cls.datname

    @hybrid_property
    def username(self):
        """
        Alias to the query's username.

        Returns
        -------
        str:
            The query's owner's username.
        """
        return self.usename

    @username.expression
    def username(cls):
        """
        SQL Wrapper for the username hybrid_property
        """
        return cls.usename

    @hybrid_property
    def transaction_start(self):
        """
        Alias to the query's xact_start.

        Returns
        -------
        datetime:
            The query's transaction instantiation date with timezone data.
        """
        return self.xact_start

    @hybrid_property
    def backend_elapsed(self):
        """
        The amount of time elapsed since the query's backend was
        created.

        Returns
        -------
        timedelta
        """
        return datetime.now() - self.backend_start

    @backend_elapsed.expression
    def backend_elapsed(cls):
        """
        SQL Wrapper for the backend_elapsed hybrid property.
        """
        return func.now() - cls.backend_start

    @hybrid_property
    def transaction_elapsed(self):
        """
        Return the amount of time elapsed since the query's transaction
        has been initialized.

        Returns
        -------
        timedelta

        """
        return datetime.now() - self.transaction_start

    @transaction_elapsed.expression
    def transaction_elapsed(cls):
        """
        SQL Wrapper for the transaction_elapsed hybrid_property.
        """
        return func.now() - cls.transcation_start

    @hybrid_property
    def query_elapsed(self):
        """
        Return the amount of time elapsed since the query was invoked.

        Returns
        -------
        timedelta
        """

        return datetime.now() - self.query_start

    @query_elapsed.expression
    def query_elapsed(cls):
        """
        SQL Wrapper for the query_elapsed hybrid property.
        """
        return func.now() - cls.query_start

    @hybrid_property
    def time_since_state_change(self):
        """
        Return the amount of time since the query's runtime state changed.

        Returns
        -------
        timedelta
        """
        return datetime.now() - self.transaction_start

    @time_since_state_change.expression
    def time_since_state_change(cls):
        """
        SQL Wrapper for the time_since_state_change hybrid_property
        """
        return func.now() - cls.transaction_start

    @hybrid_property
    def blocked_by(self):
        """
        Find existing queries which are blocking this query. May return an
        empty list if no processes are blocking the current query instance.

        Returns
        -------
        list of integers
        """
        session = inspect(self).session
        return [pid for pid in session.query(func.pg_blocking_pids(self.pid))]

    @blocked_by.expression
    def blocked_by(cls):
        """
        SQL Wrapper for the blocked_by hybrid_property.
        """
        return func.pg_blocking_pids(cls.pid)

    @classmethod
    def is_blocked(cls):
        """
        Quick helper to filter for queries that are blocked.
        """
        return func.cardinality(cls.blocked_by) > 0


# class PostgresAPIMixin(object):
#     """
#     Grant easy administration definitions to a database object.
#     """
#     def get_blocked_queries(self):
#         cardinality = func.cardinality
#         q = (
#             self
#             .query(PGStatActivity)
#             .
#         )

