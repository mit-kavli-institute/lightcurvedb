import click
import os
from lightcurvedb.core.connection import db_from_config
from lightcurvedb import models as Models
from lightcurvedb.cli.utils import resolve_filter_column
from lightcurvedb.util.sql import get_operator_f


MODEL_LOOKUP = {
    name.lower().replace("_", ""): getattr(Models, name)
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


class ModelField(click.ParamType):

    name = "model_fields"

    def __init__(self, Model, *args, **kwargs):
        super(ModelField, self).__init__(*args, **kwargs)
        self.Model = Model

    def convert(self, value, param, ctx):
        try:
            field = getattr(self.Model, value)
            return field
        except AttributeError:
            self.fail(
                "{0} does not have the attribute {1}".format(Model, value)
            )


class QLPModelType(click.ParamType):

    name = "qlp_model_type"

    def convert(self, value, param, ctx):
        sanitized = (
            value.lower().replace(" ", "").replace("-", "").replace("_", "")
        )
        try:
            return MODEL_LOOKUP[sanitized]
        except AttributeError:
            self.fail("Unknown Model {0}", param, ctx)


class ClickSQLParameter(click.ParamType):

    name = "sql_parameter"

    def convert(self, value, param, ctx):

        try:
            parsed, alias = value.strip().split(":")
        except ValueError:
            # No alias
            parsed = alias = value.strip()

        TargetModel = ctx.obj["target_model"]
        param_paths = tuple(map(lambda p: p.strip(), parsed.split(".")))

        try:
            sql_col, contexts = TargetModel.get_property(*param_paths)
        except KeyError as e:
            self.fail(e, param, ctx)
        except IndexError:
            self.fail(
                "unknown parameter on {0} with {1}".format(
                    TargetModel, param_paths
                )
            )

        sql_col = sql_col.label(alias)

        result = {
            "column": sql_col,
            "alias": alias,
        }

        current_columns = ctx.obj.get("queried_columns", {})
        current_columns[alias] = sql_col

        ctx.obj["queried_columns"] = current_columns

        if contexts:
            result.update(contexts)
        return result


class FilterParameter(click.ParamType):
    name = "sql_filter"

    def convert(self, value, param, ctx):

        model = ctx.obj["target_model"]

        if "queried_columns" not in ctx.obj:
            self.fail(
                "attempting to filter on columns when none have been "
                "specified"
            )

        try:
            lhs, op, rhs = value.split()
        except ValueError:
            self.fail(
                '"{0}" does not look like a valid '
                "filter parameter...".format(value)
            )

        # Check to see if rhs or lhs are specfying given columns
        rhs = resolve_filter_column(ctx.obj["queried_columns"], model, rhs)
        lhs = resolve_filter_column(ctx.obj["queried_columns"], model, lhs)

        clause = get_operator_f(op)(rhs, lhs)
        filters = ctx.obj.get("filters", [])

        filters.append(clause)
        ctx.obj["filters"] = filters

        return clause


class OrderParameter(click.ParamType):
    name = "order_parameter"

    def convert(self, value, param, ctx):

        if value.startswith("-"):
            parsed = value[1:]
            descending = True
        else:
            parsed = value
            descending = False

        TargetModel = ctx.obj["target_model"]
        param_paths = tuple(map(lambda p: p.strip(), parsed.split(".")))

        try:
            sql_col, contexts = TargetModel.get_property(*param_paths)
        except KeyError as e:
            self.fail(e, param, ctx)

        if descending:
            return sql_col.desc()
        return sql_col.asc()
