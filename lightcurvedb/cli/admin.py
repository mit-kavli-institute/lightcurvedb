import multiprocessing as mp

import click
import sqlalchemy as sa
from ligthcurvedb.core import partitioning
from sqlalchemy import text
from tabulate import tabulate

from lightcurvedb import db_from_config
from lightcurvedb.cli.base import lcdbcli
from lightcurvedb.cli.types import ModelField
from lightcurvedb.cli.utils import tabulate_query
from lightcurvedb.core.psql_tables import PGStatActivity


@lcdbcli.group()
@click.pass_context
def admin(ctx):
    """Base LCDB admin Commands"""
    click.echo("Entering admin context, please use responsibly!" "")


# Define procedure cli commands
@admin.group()
@click.pass_context
def procedures(ctx):
    """
    Base SQL Procedure Commands
    """


@procedures.command()
@click.pass_context
def reload(ctx):
    """
    Read the defined SQL files and submit any changes to the database.
    """
    from lightcurvedb.io.procedures.procedure import _yield_procedure_ddl

    with db_from_config(ctx.obj["dbconf"]) as db:
        for ddl in _yield_procedure_ddl():
            click.echo("Executing {0}".format(ddl))
            db.execute(ddl)
        if not ctx.obj["dryrun"]:
            click.echo("Committing...")
            db.commit()
            click.echo("Success")
        else:
            click.echo("Rolling back!")
            db.rollback()


@procedures.command()
@click.pass_context
def list_defined(ctx):
    """
    List defined PostgreSQL Stored Procedures
    """
    # TODO, define statement using SQLAlchemy constructs
    # Currently, shamelessly copied from
    # https://stackoverflow.com/questions/1347282/how-can-i-get-a-list-of-all-functions-stored-in-the-database-of-a-particular-sch
    RAW_SQL = """
        SELECT
            routines.routine_name,
            parameters.data_type,
            parameters.ordinal_position
        FROM information_schema.routines
            LEFT JOIN information_schema.parameters
            ON routines.specific_name=parameters.specific_name
            WHERE routines.specific_schema='my_specified_schema_name'
            ORDER BY routines.routine_name, parameters.ordinal_position;
    """
    with db_from_config(ctx.obj["dbconf"]) as db:
        results = db.execute(text(RAW_SQL))
        click.echo(
            tabulate(
                results,
                headers=["Routine Name", "Data Type", "Ordinal Position"],
            )
        )


@admin.group()
@click.pass_context
def maintenance(ctx):
    pass


@maintenance.command()
@click.argument("hyper-table", type=str)
@click.argument("indexer", type=str)
@click.option("--n-processes", type=int, default=16)
def cluster_hyper_table(ctx, hyper_table, indexer, n_processes):
    chunk_q = sa.select(
        sa.func.concat(
            sa.column("chunk_schema"), ".", sa.column("chunk_name")
        ).label("chunk_path")
    ).select_from(sa.func.chunks_detailed_size(hyper_table))
    jobs = []
    with db_from_config(ctx.obj["dbconf"]) as db:
        for (chunkpath,) in db.execute(chunk_q):
            jobs.append((ctx.obj["dbconf"], chunkpath, indexer))
            click.echo(f"Ordering {chunkpath} using {indexer}")
    with mp.Pool(n_processes) as pool:
        pool.starmap(partitioning.reorder_chunk, jobs)

    return


@admin.group()
@click.pass_context
def state(ctx):
    pass


@state.command()
@click.pass_context
@click.option(
    "--column",
    "-c",
    "columns",
    multiple=True,
    type=ModelField(PGStatActivity),
    default=["pid", "state", "application_name", "query"],
)
def get_all_queries(ctx, columns):
    with db_from_config(ctx.obj["dbconf"]) as db:
        q = db.query(*columns).filter(
            PGStatActivity.database == "lightpointdb"
        )
        click.echo(tabulate_query(q))


@state.command()
@click.pass_context
@click.option(
    "--column",
    "-c",
    "columns",
    multiple=True,
    type=ModelField(PGStatActivity),
    default=["pid", "query", "application_name", "blocked_by"],
)
def get_blocked_queries(ctx, columns):
    with db_from_config(ctx.obj["dbconf"]) as db:
        q = db.query(*columns).filter(
            PGStatActivity.database == "lightpointdb",
            PGStatActivity.is_blocked(),
        )
        click.echo(tabulate_query(q))


@state.command()
@click.pass_context
@click.argument("pids", type=int, nargs=-1)
@click.option(
    "--column",
    "-c",
    "columns",
    multiple=True,
    type=ModelField(PGStatActivity),
    default=["pid", "state", "application_name", "query"],
)
def get_info(ctx, pids, columns):
    with db_from_config(ctx.obj["dbconf"]) as db:
        q = db.query(*columns).filter(PGStatActivity.pid.in_(pids))
        click.echo(tabulate_query(q))


@state.command()
@click.pass_context
@click.argument("pids", type=int, nargs=-1)
def terminate(ctx, pids):
    with db_from_config(ctx.obj["dbconf"]) as db:
        queries = db.query(PGStatActivity.query).filter(
            PGStatActivity.pid.in_(pids)
        )

        if queries.count() == 0:
            click.echo("No queries with pids {pids} exist")
            return 0

        click.echo(click.style("Will terminate..."))
        for (query,) in queries:
            click.echo(f"\t{query}")
        prompt_msg = click.style(
            "TERMINATE THESE QUERIES?", bg="red", blink=True
        )

        click.confirm(prompt_msg, abort=True, default=False)

        db.query(PGStatActivity.terminate).filter(
            PGStatActivity.pid.in_(pids)
        ).all()
        click.echo("Terminated")
