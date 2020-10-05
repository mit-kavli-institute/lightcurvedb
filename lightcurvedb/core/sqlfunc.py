# Since SQLAlchemy does not support postgres constructs like
# WITH CARDINALITY, this module hacks that functionality into
# SQLAlchemy.
# Credit to https://gist.github.com/z0u/0b7aab4449e58f4d4a66

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import functions
from sqlalchemy.sql.elements import ColumnClause
from sqlalchemy.sql.selectable import FromClause

class FunctionColumn(ColumnClause):
    def __init__(self, function, name, type_=None):
        self.function = self.table = function
        self.name = self.key = name
        self.type_ = type_
        self.is_literal = False

    @property
    def _from_objects(self):
        return []

    def _make_proxy(self, selectable, name=False, attach=True,
            name_is_truncatable=False, **kw):
        raise NotImplementedError
