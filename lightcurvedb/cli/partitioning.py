from __future__ import division, print_function

from sys import exit
from tqdm import tqdm

import click

import lightcurvedb.models as defined_models
from lightcurvedb.models.table_track import RangedPartitionTrack
from lightcurvedb.cli.base import lcdbcli
from lightcurvedb.core.partitioning import emit_ranged_partition_ddl
from lightcurvedb.core.admin import psql_tables
from sqlalchemy import text


@lcdbcli.group()
@click.pass_context
def partitioning(ctx):
    """
    Commands for adding and adjusting Partitioned Tables.
    """
    if ctx.obj["dryrun"]:
        click.echo(
            click.style(
                "Running in dryrun mode. No partitions will altered!",
                fg="green",
                bold=True,
            )
        )


@partitioning.command()
@click.pass_context
@click.argument("model", type=str)
def list_partitions(ctx, model):
    """
    List the partitions of MODEL
    """
    with ctx.obj["dbconf"] as db:
        psql_tables(db)
        # Get current partition of the table.
        try:
            target_model = getattr(defined_models, model)
        except AttributeError:
            click.echo("No known model {0}".format(model))
            exit(1)

        partitions = db.get_partitions_df(target_model)
        click.echo(partitions.sort_values("end_range"))
        click.echo(
            "A total of {0} partitions!".format(
                click.style(str(len(partitions)), bold=True)
            )
        )


@partitioning.command()
@click.pass_context
@click.argument("model", type=str)
@click.argument("number_of_new_partitions", type=click.IntRange(min=1))
@click.argument("blocksize", type=click.IntRange(min=1))
@click.option(
    "--schema",
    type=str,
    default="partitions",
    help="Schema space to place the partition under.",
)
def create_partitions(ctx, model, number_of_new_partitions, blocksize, schema):
    """
    Create ranged partitions on the given MODEL with ranges equivalent to the
    BLOCKSIZE.
    """
    with ctx.obj["dbconf"] as db:
        # Get current partition of the table.
        psql_tables(db)
        try:
            target_model = getattr(defined_models, model)
        except AttributeError:
            click.echo("No known model {0}".format(model))
            exit(1)

        partitions = db.get_partitions_df(target_model)
        if len(partitions) > 0:
            current_max = max(partitions["end_range"])
        else:
            current_max = 0

        if current_max is None:
            click.echo(
                "Model {0} has no partitions! Please define a partition "
                "rule in the PSQL shell".format(model)
            )
            exit(1)

        new_partition_models = []

        for _ in range(number_of_new_partitions):
            ddl = emit_ranged_partition_ddl(
                target_model.__tablename__,
                current_max,
                current_max + blocksize,
                schema=schema,
            )
            partition_name = "{0}_{1}_{2}".format(
                target_model.__tablename__,
                current_max,
                current_max + blocksize,
            )

            info = {
                "partition_name": partition_name,
                "min_range": current_max,
                "max_range": current_max + blocksize,
                "model": target_model.__name__,
            }

            new_partition_models.append((ddl, info))

            click.echo(
                "\tWill emit new PARTITION FROM VALUES "
                "({0}) TO ({1})".format(current_max, current_max + blocksize)
            )
            current_max += blocksize

        try:
            original_begin = max(partitions["end_range"])
        except ValueError:
            original_begin = 0
        click.echo(
            "Will create {0} partitions spanning values from "
            "{1} to {2}".format(
                len(new_partition_models), original_begin, current_max
            )
        )

        # Determine if blocksize is an anomaly
        blocksizes = partitions["end_range"] - partitions["begin_range"]
        stddev = blocksizes.std()
        mean = blocksizes.mean()

        # Check if blocksize looks odd
        strange_blocksize = not (mean - stddev <= blocksize <= mean + stddev)

        if strange_blocksize:
            click.echo(
                click.style(
                    "Odd blocksize!",
                    bold=True,
                    fg="white",
                    bg="red",
                    blink=True,
                )
            )
            click.echo(
                "Specified blocksize {0} "
                "resides outside avg blocksize {1} +- {2}".format(
                    blocksize, mean, stddev
                )
            )

        if not ctx.obj["dryrun"]:
            click.confirm("Do the following changes look okay?", abort=True)
            for partition, info in tqdm(
                new_partition_models, desc="Creating Tables..."
            ):
                db.session.execute(partition)
                tablename = info.pop("partition_name")

                oid = db.get_pg_oid(tablename)

                track = RangedPartitionTrack(oid=oid, **info)
                db.add(track)

            db.commit()
            click.echo(
                "Committed {0} new partitions!".format(
                    len(new_partition_models)
                )
            )


@partitioning.command()
@click.pass_context
@click.argument("model", type=str)
@click.option("--pattern", "-p", type=str, default=".*")
def delete_partitions(ctx, model, pattern):
    with ctx.obj["dbconf"] as db:
        # Get current partition of the table.
        try:
            target_model = getattr(defined_models, model)
        except AttributeError:
            click.echo("No known model {0}".format(model))
            exit(1)

        partitions = db.get_partitions_df(target_model)
        if len(partitions) > 0:
            current_max = max(partitions["end_range"])
        else:
            current_max = 0

        if current_max is None:
            click.echo(
                "Model {0} has no partitions! "
                "Please define a partition rule "
                "in the PSQL shell".format(model)
            )
            exit(1)
        names = partitions["partition_name"]
        mask = names.str.contains(pattern)
        names = names[mask]

        click.echo(names)
        click.echo("Will remove {0} partitions!".format(len(names)))
        if not ctx.obj["dryrun"]:
            click.confirm("Does this look okay?", abort=True)
            for name in names:
                q = text("DROP TABLE {0}".format(name))
                db.session.execute(q)
                db.commit()
                click.echo("\tDeleted {0}".format(name))


def schemaed_table(table, schema):
    return "{0}.{1}".format(schema, table) if schema else table


@partitioning.command()
@click.pass_context
@click.argument("model", type=str)
@click.option("--pattern", "-p", type=str, default=".*")
@click.option("--schema", type=str, default="partitions")
def set_unlogged(ctx, model, pattern, schema):
    try:
        target_model = getattr(defined_models, model)
    except AttributeError:
        click.echo("No known model {0}".format(model))
        exit(1)
    with ctx.obj["dbconf"] as db:
        partitions = db.get_partitions_df(target_model)
        tablenames = list(partitions.partition_name)

        for table in tablenames:
            table = schemaed_table(table, schema)
            q = text("ALTER TABLE {0} SET UNLOGGED".format(table))
            click.echo("Altering {0}".format(click.style(table, bold=True)))
            db.session.execute(q)
            db.commit()
        click.echo("Altered {0} tables! Done".format(len(tablenames)))


@partitioning.command()
@click.pass_context
@click.argument("model", type=str)
@click.option("--pattern", "-p", type=str, default=".*")
@click.option("--schema", type=str, default="partitions")
def set_logged(ctx, model, pattern, schema):
    try:
        target_model = getattr(defined_models, model)
    except AttributeError:
        click.echo("No known model {0}".format(model))
        exit(1)

    with ctx.obj["dbconf"] as db:
        partitions = db.get_partitions_df(target_model)
        tablenames = list(partitions.partition_name)

        for table in tablenames:
            table = schemaed_table(table, schema)
            q = text("ALTER TABLE {0} SET LOGGED".format(table))
            click.echo("Altering {0}".format(click.style(table, bold=True)))
            db.session.execute(q)
            db.commit()
        click.echo("Altered {0} tables! Done".format(len(tablenames)))
