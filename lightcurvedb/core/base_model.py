from __future__ import division, print_function

from lightcurvedb.core.admin import get_psql_catalog_tables
from sqlalchemy import Column, DateTime, String, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.ext.declarative import as_declarative
from sqlalchemy.sql import func


@as_declarative()
class QLPModel(object):
    """
    Common SQLAlchemy base model for all QLP Models
    """
    __abstract__ = True

    @classmethod
    def insert(cls, *args, **kwargs):
        """
        Return a SQLAlchemy Core query object representing an insert statement
        for this model.
        """
        return insert(cls.__table__, *args, **kwargs)

    @hybrid_property
    def oid(self):
        pg_class = get_psql_catalog_tables('pg_class')
        return select(
            [pg_class.c.oid]
        ).where(pg_class.c.relname == self.__tablename__)

    @oid.expression
    def oid(cls):
        pg_class = get_psql_catalog_tables('pg_class')
        print(cls.__tablename__)
        return select(
            [pg_class.c.oid]
        ).where(pg_class.c.relname == cls.__tablename__).label('oid')


class QLPDataProduct(QLPModel):
    """
    Mixin for describing QLP Dataproducts such as frames, lightcurves,
    and BLS results
    """
    __abstract__ = True

    created_on = Column(DateTime, server_default=func.now())


class QLPDataSubType(QLPModel):
    """
    Mixin for describing QLP data subtypes such as lightcurve types.
    """
    __abstract__ = True

    name = Column(String(64), primary_key=True, nullable=False)
    description = Column(String)
    created_on = Column(DateTime, server_default=func.now())

    @property
    def id(self):
        return self.name


class QLPReference(QLPModel):
    """
        Mixin for describing models which are used by QLP but generally aren't
        'produced'. For example: orbits and space craft telemetery
    """
    __abstract__ = True

    created_on = Column(DateTime, server_default=func.now())


class QLPMetric(QLPModel):
    """
    Mixin for describing models which are purely for determining performance
    metrics or some other internal diagnostics.
    """

    __abstract__ = True
    created_on = Column(DateTime, server_default=func.now())
