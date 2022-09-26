from __future__ import division, print_function

from sqlalchemy import Column, DateTime, String, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import ColumnProperty, RelationshipProperty, as_declarative, declarative_mixin
from sqlalchemy.sql import func

from lightcurvedb.core.admin import get_psql_catalog_tables
from lightcurvedb.core.psql_tables import PGClass
from lightcurvedb.core.sql import psql_safe_str


@as_declarative()
class QLPModel:
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
        return (
            select([PGClass.oid])
            .where(PGClass.relname == cls.__tablename__)
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
            raise KeyError("give path {0} is empty".format(paths))
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
@declarative_mixin
class CreatedOnMixin:
    """
    Mixin for describing QLP Dataproducts such as frames, lightcurves,
    and BLS results
    """
    created_on = Column(DateTime, server_default=func.now())


@declarative_mixin
class NameAndDescriptionMixin:
    """
    Mixin for describing QLP data subtypes such as lightcurve types.
    """

    _name = Column("name", String(64), unique=True, nullable=False)
    _description = Column("description", String)

    @hybrid_property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = psql_safe_str(value)

    @name.expression
    def name(cls):
        return cls._name

    @hybrid_property
    def description(self):
        return self._description

    @description.setter
    def description(self, value):
        self._description = psql_safe_str(value)

    @description.expression
    def description(cls):
        return cls._description
