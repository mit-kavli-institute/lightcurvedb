from sqlalchemy.schema import Table, Column
from sqlalchemy.orm import Query
from sqlalchemy.inspection import inspect
from sqlalchemy.sql import compiler
from lightcurvedb.core.base_model import QLPModel
import os
from datetime import datetime


class TempTable(object):
    def __init__(self, name, *columns):
        self.name = name
        self._sql_table = Table(
            name,
            QLPModel.metadata
        )

class TemporaryTableQuery(Query):
    """Allows emission of a temporary table from a given query"""
    def compile(self):
        statement = self.statement
        engine = self.session.get_bind()
        dialect = engine.dialect
        encoder = dialect.encoding
        compiled = compiler.SQLCompiler(dialect, statement)

        connection = engine.raw_connection().connection
        raise NotImplementedError


class MassQuery(object):
    def __init__(self, session, TargetModel, pk_col, *cols, **additional_filters):
        self.session = session
        self.Model = TargetModel

        time = str(datetime.now()).replace(':', '_').replace('.','_').replace(' ', '_').replace('-','_')
        name = '{}_{}'.format(os.getpid(), time)
        self.name = '{}_massquery_{}'.format(TargetModel.__table__.name, name)
        temp_table = Table(
            self.name,
            QLPModel.metadata,
            *cols,
            prefixes=['TEMPORARY']  #Sets temp table,
        )
        temp_table.create(bind=session.bind)
        self.session.commit()
        if pk_col is not None:
            self.left_key = pk_col
        else:
            self.left_key = inspect(TargetModel).primary_key
        self.right_key_name = self.left_key.name
        self.table = temp_table
        self.filters = additional_filters

    def execute(self):
        q = self.session.query(
            self.Model
        ).filter_by(**self.filters).join(
            self.table,
            self.left_key == self.table.c[self.right_key_name]
        )
        return q

    def insert(self, **data):
        cmd = self.table.insert().values(**data)
        self.session.execute(cmd)


    def mass_insert(self, values):
        q = self.table.insert().values(values)
        self.session.execute(q)




