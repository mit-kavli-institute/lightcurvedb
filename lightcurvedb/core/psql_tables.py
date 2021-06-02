"""
This module re-defines internal PostgreSQL catalog tables for fast
reflection.

This table should be modified depending on which version of PostgreSQL
is used.
"""

from sqlalchemy import (
    Column,
    Integer,
    String,
    Text,
    DateTime,
    inspect,
    Boolean,
    Float,
    ForeignKey,
)

from sqlalchemy.types import CHAR
from sqlalchemy.dialects.postgresql import OID, INET
from sqlalchemy.ext.declarative import as_declarative
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.sql import func
from datetime import datetime


# Alias referenced datatypes to better match internal datatypes
XID = Integer
NAME = String(64)


@as_declarative()
class PGStatModel(object):
    """
    Wrapper for PG Statistical Catalogs
    """

    __abstract__ = True


@as_declarative()
class PGCatalogModel(object):
    """
    Wrapper for PG Catalogs
    """

    __abstract__ = True


"""
##############
Begin PGState Model definitions
##############
"""


class PGStatActivity(PGStatModel):
    """
    Wraps behavior around PostgreSQL's pg_stat_activity.
    """

    __tablename__ = "pg_stat_activity"

    pid = Column(Integer, primary_key=True)

    datid = Column(OID)
    datname = Column(NAME)
    usesysid = Column(OID)
    usename = Column(NAME)
    application_name = Column(Text)

    client_addr = Column(INET)
    client_hostname = Column(Text)
    client_port = Column(Integer)

    backend_start = Column(DateTime(timezone=True))
    xact_start = Column(DateTime(timezone=True))
    query_start = Column(DateTime(timezone=True))
    state_change = Column(DateTime(timezone=True))

    wait_event_type = Column(NAME)
    wait_event = Column(NAME)

    state = Column(NAME)

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


"""
##############
Begin PGCatalog Model definitions
##############
"""


class PGAuthID(PGCatalogModel):
    """
    Wraps behavior around postgresql's pg_authid catalog.
    """

    __tablename__ = "pg_authid"

    oid = Column(OID, primary_key=True)
    rolname = Column(NAME)
    rolsuper = Column(Boolean)
    rolinherit = Column(Boolean)
    rolcreaterole = Column(Boolean)
    rolcreatedb = Column(Boolean)
    rolcanlogin = Column(Boolean)


class PGNamespace(PGCatalogModel):
    """
    Wraps behavior around postgresql's pg_namespace catalog.
    """

    __tablename__ = "pg_namespace"

    oid = Column(OID, primary_key=True)
    nspname = Column(NAME)
    nspowner = Column(ForeignKey(PGAuthID.__tablename__ + ".oid"))

    owner = relationship(PGAuthID, backref="namespaces")


class PGType(PGCatalogModel):
    """
    Wraps behavior around postgresql's pg_type catalog.
    """

    __tablename__ = "pg_type"

    oid = Column(OID, primary_key=True)
    typname = Column(NAME)
    typnamespace = Column(OID, ForeignKey(PGNamespace.__tablename__ + ".oid"))


class PGInherits(PGCatalogModel):
    """
    Wraps behavior around Many to Many PGInherits catalog.
    """

    __tablename__ = "pg_inherits"

    inhrelid = Column(OID, ForeignKey("pg_class.oid"), primary_key=True)
    inhparent = Column(OID, ForeignKey("pg_class.oid"), primary_key=True)

    @hybrid_property
    def child_oid(self):
        return self.inhrelid

    @child_oid.expression
    def child_oid(cls):
        return cls.inhrelid

    @hybrid_property
    def parent_oid(self):
        return self.inhparent

    @parent_oid.expression
    def parent_oid(cls):
        return cls.inhparent


class PGIndex(PGCatalogModel):
    """
    Wraps behavior around postgresql's pg_indexes catalog.
    """

    __tablename__ = "pg_indexes"

    schemaname = Column(NAME, ForeignKey(PGNamespace.nspname), primary_key=True)
    tablename = Column(NAME, ForeignKey("pg_class.relname"))
    indexname = Column(NAME, ForeignKey("pg_class.relname"))


class PGClass(PGCatalogModel):
    """
    Wraps behavior around postgresql's pg_class catalog.
    """

    __tablename__ = "pg_class"

    oid = Column(OID, primary_key=True)
    relname = Column(NAME)
    reltype = Column(OID, ForeignKey(PGType.__tablename__ + ".oid"))
    relowner = Column(ForeignKey(PGAuthID.__tablename__ + ".oid"))
    relpages = Column(Integer)
    reltuples = Column(Float)
    relkind = Column(CHAR)
    relispartition = Column(Boolean)
    relpartbound = Column(Text)

    type_ = relationship(PGType, backref="classes")
    owner = relationship(PGAuthID, backref="classes")
    parent = relationship(
        "PGClass",
        secondary=PGInherits.__table__,
        primaryjoin=(oid == PGInherits.child_oid),
        secondaryjoin=(oid == PGInherits.parent_oid),
        single_parent=True,
        backref=backref("children"),
    )
    index_parent = relationship(
        "PGClass",
        secondary=PGIndex.__table__,
        primaryjoin=(relname == PGIndex.indexname),
        secondaryjoin=(relname == PGIndex.tablename),
        single_parent=True,
        backref=backref("indexes")
    )

    @classmethod
    def expression(cls):
        return func.pg_get_expr(cls.relpartbound, cls.oid).label("expression")


class PGCatalogMixin(object):
    """
    Mixing to provide database objects PGCatalog API methods.
    """

    def get_pg_oid(self, tablename):
        """
        Obtain postgres's internal OID for the given tablename.

        Returns
        -------
        int or None:
            Returns the OID (int) of the given table or ``None`` if no such
            table exists.
        """
        return (
            self.query(PGClass.oid).filter_by(relname=tablename).one_or_none()
        )
