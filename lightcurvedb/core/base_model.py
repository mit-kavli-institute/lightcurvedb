from __future__ import division, print_function

from lightcurvedb.core.admin import get_psql_catalog_tables
from sqlalchemy import Column, DateTime, String, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.ext.declarative import as_declarative
from sqlalchemy.sql import func
from sqlalchemy.orm import ColumnProperty, RelationshipProperty


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
        pg_class = get_psql_catalog_tables("pg_class")
        return select([pg_class.c.oid]).where(
            pg_class.c.relname == self.__tablename__
        )

    @oid.expression
    def oid(cls):
        pg_class = get_psql_catalog_tables("pg_class")
        print(cls.__tablename__)
        return (
            select([pg_class.c.oid])
            .where(pg_class.c.relname == cls.__tablename__)
            .label("oid")
        )

    @classmethod
    def get_columns(cls):
        return tuple(col.name for col in cls.__table__.columns)

    @classmethod
    def get_property(cls, *paths, **kwargs):
        """
        Traverse the relationship property graph and return
        the endpoint ColumnProperty as well as any needed
        JOIN contexts.

        Parameters
        ----------
        *paths : variable length str
            The order of parameters to attempt to traverse down.
        **kwargs : keyword arguments
            TODO Add contextual support

        Raises
        ------
        KeyError
            Raised if any of the given paths does not result in a Column
            or Relationship Property.
        """
        try:
            path = paths[0]
        except IndexError:
            raise KeyError(
                "give path {0} is empty".format(paths)
            )
        remainder = paths[1:]
        try:
            class_attr = getattr(cls, path)
            attr = class_attr.property
            if isinstance(attr, RelationshipProperty):
                # Use remainder to parse related property
                RelatedClass = attr.mapper.class_
                # Assume all related classes are of QLPModel.
                join_contexts = kwargs.get("join_contexts", set())
                if attr not in join_contexts:
                    join_contexts.add(attr)

                kwargs["join_contexts"] = join_contexts

                return RelatedClass.get_property(*remainder, **kwargs)
            elif isinstance(attr, ColumnProperty):
                return class_attr, kwargs
            else:
                raise AttributeError(
                    "Path '{0}' is a property/method on {1} but is not an SQL"
                    "tracked property.".format(path, cls)
                )

        except AttributeError:
            raise KeyError(
                "Could not find any SQL properties on {0} with the "
                "path '{1}'".format(cls, path)
            )


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
