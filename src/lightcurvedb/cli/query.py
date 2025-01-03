from itertools import chain

import click
import pandas as pd
from tabulate import tabulate

from lightcurvedb import db_from_config
from lightcurvedb.cli.base import lcdbcli
from lightcurvedb.cli.types import (
    ClickSQLParameter,
    FilterParameter,
    OrderParameter,
    QLPModelType,
)

try:
    from io import StringIO
except ImportError:
    # Python 2!
    from StringIO import StringIO


@lcdbcli.group()
@click.pass_context
@click.argument("model", type=QLPModelType())
def query(ctx, model):
    """
    Commands for executing subsets of SQL and displaying tabular data.
    """
    ctx.obj["target_model"] = model


@query.command()
@click.pass_context
@click.option(
    "--parameter", "-p", "parameters", type=ClickSQLParameter(), multiple=True
)
@click.option(
    "--filter", "-f", "filters", type=FilterParameter(), multiple=True
)
@click.option(
    "--order-by", "-O", "orders", type=OrderParameter(), multiple=True
)
@click.option("--table-fmt", type=str, default="plain")
@click.option("--header/--no-header", default=True)
def print_table(ctx, parameters, filters, orders, table_fmt, header):
    # Construct an SQL query given the cli parameters
    with db_from_config(ctx.obj["dbconf"]) as db:
        cols = tuple(col["column"] for col in parameters)
        names = tuple(col["alias"] for col in parameters)

        # col_map = {col["alias"]: col["column"] for col in cols}

        relation_path_bundles = tuple(
            col["join_contexts"]
            for col in parameters
            if "join_contexts" in col
        )
        q = db.query(*cols)

        # Traverse JOIN requirements
        join_reqs = set(chain.from_iterable(relation_path_bundles))
        for join_clause in join_reqs:
            q = q.join(join_clause)

        # Perform filters
        if filters:
            q = q.filter(*filters)

        # Perform orders
        if orders:
            q = q.order_by(*orders)

        results = q.all()

    # If CSV fmt, don't pass into tabulate
    if table_fmt == "csv":
        df = pd.DataFrame(results, columns=names)
        output = StringIO()
        df.to_csv(output, index=False, header=header)
        output.seek(0)
        click.echo(output.read())
    else:
        headers = names if header else ()
        click.echo(tabulate(results, headers=headers, tablefmt=table_fmt))
