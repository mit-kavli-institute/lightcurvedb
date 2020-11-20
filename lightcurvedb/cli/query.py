import os
import click

from lightcurvedb.cli.base import lcdbcli
from lightcurvedb.cli.types import QLPModelType, ClickSQLParameter
from itertools import chain
from tabulate import tabulate
import pandas as pd

try:
    from io import StringIO
except ImportError:
    # Python 2!
    from StringIO import StringIO


@lcdbcli.group()
@click.pass_context
@click.argument('Model', type=QLPModelType())
def query(ctx, Model):
    ctx.obj["target_model"] = Model


@query.command()
@click.pass_context
@click.option('--parameter', '-p', 'parameters', type=ClickSQLParameter(), multiple=True)
@click.option('--filter', '-f', 'filters', type=str, multiple=True)
@click.option('--table-fmt', type=str, default="plain")
def print_table(ctx, parameters, filters, table_fmt):
    # Construct an SQL query given the cli parameters
    with ctx.obj["dbconf"] as db:
        cols = tuple(col["column"] for col in parameters)
        names = tuple(col["alias"] for col in parameters)

        # col_map = {col["alias"]: col["column"] for col in cols}

        relation_path_bundles = tuple(col["join_contexts"] for col in parameters if "join_contexts" in col)
        q = db.query(*cols)

        # Traverse JOIN requirements
        join_reqs = set(chain.from_iterable(relation_path_bundles))
        for join_clause in join_reqs:
            q = q.join(join_clause)

        # Perform filters
        # TODO

        results = q.all()

    # If CSV fmt, don't pass into tabulate
    if table_fmt == 'csv':
        df = pd.DataFrame(results, columns=names)
        output = StringIO()
        df.to_csv(output)
        output.seek(0)
        click.echo(output.read())
    else:
        click.echo(
            tabulate(
                results,
                tablefmt=table_fmt
            )
        )
