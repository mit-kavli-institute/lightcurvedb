import click
import os
from lightcurvedb.core.connection import db_from_config
from lightcurvedb import models as Models
from sqlalchemy.orm import ColumnProperty, RelationshipProperty


MODEL_LOOKUP = {
    name.lower().replace('_', ''): getattr(Models, name)
    for name in Models.DEFINED_MODELS
}


class CommaList(click.ParamType):

    name = "comma_list"

    def __init__(self, type, *args, **kwargs):
        super(CommaList, self).__init__(*args, **kwargs)
        self.split_type = type

    def convert(self, value, param, ctx):
        try:
            parameters = value.split(",")
            return tuple(
                self.split_type(parameter) for parameter in parameters
            )
        except ValueError:
            self.fail("", param, ctx)
        except TypeError:
            self.fail("", param, ctx)


class Database(click.ParamType):

    name = "dbconfig"

    def convert(self, value, param, ctx):
        if not value:
            value = os.path.expanduser("~/.config/lightcurvedb/db.conf")
        return db_from_config(
            value,
            executemany_mode="values",
            executemany_values_page_size=10000,
            executemany_batch_page_size=500,
        )


class QLPModelType(click.ParamType):

    name = "qlp_model_type"

    def convert(self, value, param, ctx):
        sanitized = (
            value
            .lower()
            .replace(' ', '')
            .replace('-', '')
            .replace('_', '')
        )
        try:
            return MODEL_LOOKUP[sanitized]
        except AttributeError:
            self.fail(
                "Unknown Model {0}",
                param,
                ctx
            )


class ClickSQLParameter(click.ParamType):

    name = "sql_parameter"

    def convert(self, value, param, ctx):
        try:
            param, alias = value.strip().split(':')
        except ValueError:
            # No alias
            param = alias = value.strip()

        TargetModel = ctx.obj["target_model"]
        param_paths = tuple(map(lambda p: p.strip(), param.split('.')))

        try:
            sql_col, contexts = TargetModel.get_property(*param_paths)
        except KeyError as e:
            self.fail(e, param, ctx)

        result = {
            'column': sql_col.label(alias),
            'alias': alias,
        }
        if contexts:
            result.update(contexts)
        return result


class FilterParameter(click.ParamType):
    name = "sql_filter"

    def convert(self, value, param, ctx):
        try:
            column, op, cmpr = value.split()
        except ValueError:
            pass

        # TODO implement small lightweight grammar
        return value
