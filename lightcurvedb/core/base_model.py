from __future__ import print_function, division
import datetime
from sqlalchemy.ext.declarative import declared_attr, as_declarative, has_inherited_table
from sqlalchemy.orm import relationship
from sqlalchemy import Column, String, DateTime, BigInteger, ForeignKey, Sequence
from sqlalchemy.schema import UniqueConstraint
import numpy as np
from psycopg2.extensions import register_adapter, AsIs


# Forward Declare Type mappings for psycopg2 to understand numpy types
#def __adapt_np__(np_type):
#    def __adaptor__(type_inst):
#        return AsIs(type_inst)
#
#register_adapter(np.int32, __adapt_np__(int))
#register_adapter(np.int64, __adapt_np__(int))
#register_adapter(np.float32, __adapt_np__(float))
#register_adapter(np.float64, __adapt_np__(float))


@as_declarative()
class QLPModel(object):
    """
        Common SQLAlchemy base model for all QLP Models
    """
    __abstract__ = True



class QLPDataProduct(QLPModel):
    """
        Mixin for describing QLP Dataproducts such as frames, lightcurves, and BLS results
    """
    __abstract__ = True

    created_on = Column(DateTime, default=datetime.datetime.utcnow)


class QLPDataSubType(QLPModel):
    """
        Mixin for describing QLP data subtypes such as lightcurve types.
    """
    __abstract__ = True

    name = Column(String(64), primary_key=True, nullable=False)
    description = Column(String)
    created_on = Column(DateTime, default=datetime.datetime.utcnow)

    @property
    def id(self):
        return self.name


class QLPReference(QLPModel):
    """
        Mixin for describing models which are used by QLP but generally aren't
        'produced'. For example: orbits and space craft telemetery
    """
    __abstract__ = True

    created_on = Column(DateTime, default=datetime.datetime.utcnow)
