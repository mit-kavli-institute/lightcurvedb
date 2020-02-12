from __future__ import print_function, division
import datetime
from sqlalchemy.ext.declarative import declared_attr, as_declarative, has_inherited_table
from sqlalchemy.orm import relationship
from sqlalchemy import Column, String, DateTime, BigInteger, ForeignKey, Sequence
from sqlalchemy.schema import UniqueConstraint


@as_declarative()
class QLPModel(object):
    """
        Common SQLAlchemy base model for all QLP Models
    """
    __abstract__ = True

    created_on = Column(DateTime, default=datetime.datetime.utcnow)

def DynamicIdMixin(tablename):
    class HasIdMixin(object):
        @declared_attr.cascading
        def id(cls):
            if has_inherited_table(cls):
                return Column(
                    ForeignKey('{}.id'.format(tablename)),
                    primary_key=True)
            else:
                return Column(
                    BigInteger, 
                    Sequence('{}_pk_table'.format(tablename)), 
                    primary_key=True)

    return HasIdMixin


class QLPDataProduct(QLPModel, DynamicIdMixin('qlpdataproducts')):
    """
        Mixin for describing QLP Dataproducts such as frames, lightcurves, and BLS results
    """
    __tablename__ = 'qlpdataproducts'

    product_type = Column(String(255))

    # This mapper setup allows polymoprhic behavior when querying on all data
    # products. (When requesting on Dataproducts heterogenous results are returned)
    @declared_attr
    def __mapper_args__(cls):
        if cls.__name__ == 'QLPDataProduct':
            return {
                'polymorphic_on': cls.product_type
            }
        else:
            return {'polymorphic_identity': cls.__tablename__}


class QLPDataSubType(QLPModel, DynamicIdMixin('qlpdatasubtypes')):
    """
        Mixin for describing QLP data subtypes such as lightcurve types.
    """
    __tablename__ = 'qlpdatasubtypes'

    name = Column(String(64), index=True, nullable=False)
    description = Column(String)
    subtype = Column(String(255))

    __table_args__ = (
        UniqueConstraint('subtype', 'name'),
    )

    @declared_attr
    def __mapper_args__(cls):
        if cls.__name__ == 'QLPDataSubType':
            return {
                'polymorphic_on': cls.subtype
            }
        else:
            return {
                'polymorphic_identity': cls.__tablename__
            }


class QLPReference(QLPModel, DynamicIdMixin('qlpreferences')):
    """
        Mixin for describing models which are used by QLP but generally aren't
        'produced'. For example: orbits and space craft telemetery
    """
    __tablename__ = 'qlpreferences'
    reference_type = Column(String(255))

    @declared_attr
    def __mapper_args__(cls):
        if cls.__name__ == 'QLPReference':
            return {
                'polymorphic_on': cls.reference_type
            }
        else:
            return {
                'polymorphic_identity': cls.__tablename__
            }
