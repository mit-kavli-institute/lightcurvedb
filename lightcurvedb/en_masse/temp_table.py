from sqlalchemy import Table
from sqlalchemy.inspection import inspect
from lightcurvedb.core.base_model import QLPModel
import os


class MassQuery(object):
    def __init__(self, session, TargetModel, pk_col, *cols, **additional_filters):
        self.session = session
        self.Model = TargetModel
        self.name = '{}_massquery_{}'.format(TargetModel.__table__.name, os.getpid())
        temp_table = Table(
            self.name
            QLPModel.metadata,
            *cols,
            prefixes=['TEMPORARY']  #Sets temp table
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
