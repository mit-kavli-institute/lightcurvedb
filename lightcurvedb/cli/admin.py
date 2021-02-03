import click
from sqlalchemy import text
from tabulate import tabulate

from lightcurvedb.experimental.io.procedures.procedure import (
    _yield_procedure_ddl,
)

from . import lcdbcli


@lcdbcli.group()
@click.pass_context
def administration(ctx):
    """Base LCDB Administration Commands"""
    click.echo("Entering administration context, please use responsibly!" "")


# Define procedure cli commands
@administration.group()
@click.pass_context
def procedures(ctx):
    pass


@procedures.command()
@click.pass_context
def reload(ctx):
    with ctx.obj["dbconf"] as db:
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
    with ctx.obj["dbconf"] as db:
        results = db.execute(text(RAW_SQL))
        click.echo(
            tabulate(
                results,
                headers=["Routine Name", "Data Type", "Ordinal Position"],
            )
        )
