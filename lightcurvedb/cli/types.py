import click
import os
from lightcurvedb.core.connection import db_from_config


class CommaList(click.ParamType):

    name = 'comma_list'

    def __init__(self, type, *args, **kwargs):
        super(CommaList, self).__init__(*args, **kwargs)
        self.split_type = type

    def convert(self, value, param, ctx):
        try:
            parameters = value.split(',')
            return tuple(self.split_type(parameter) for parameter in parameters)
        except ValueError:
            self.fail('', param, ctx)
        except TypeError:
            self.fail('', param, ctx)


class Database(click.ParamType):

    name = 'dbconfig'

    def convert(self, value, param, ctx):
        if not value:
            value = os.path.expanduser('~/.config/lightcurvedb/db.conf')
        return db_from_config(
                value,
                executemany_mode='values',
                executemany_values_page_size=10000,
                executemany_batch_page_size=500)
