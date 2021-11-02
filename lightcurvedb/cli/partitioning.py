from __future__ import division, print_function

from sys import exit
from tqdm import tqdm

import click

import lightcurvedb.models as defined_models
from lightcurvedb.models.table_track import RangedPartitionTrack
from lightcurvedb.cli.base import lcdbcli
from lightcurvedb.core.partitioning import emit_ranged_partition_ddl
from lightcurvedb.core.psql_tables import PGClass
from lightcurvedb.models.table_track import RangedPartitionTrack
from lightcurvedb.cli.types import QLPModelType
from lightcurvedb.util.iter import chunkify
from lightcurvedb.util import merging
from loguru import logger
from sqlalchemy import text
from multiprocessing import Pool


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


@partitioning.command()
@click.pass_context
@click.argument("model", type=QLPModelType())
@click.argument("minimum_length", type=int)
@click.option("--n-threads", "-n", type=int, default=0)
def merge_partitions(ctx, model, minimum_length, n_threads):
    with ctx.obj["dbconf"] as db:
        working_group = (
            db.query(RangedPartitionTrack)
            .filter(
                RangedPartitionTrack.length < minimum_length,
                RangedPartitionTrack.same_model(model),
            )
            .order_by(RangedPartitionTrack.min_range)
        )

        pairs = []
        for left, *remainder in chunkify(working_group, 2):
            if remainder:
                pairs.append(merging.WorkingPair(left.oid, remainder[0].oid))
            else:
                click.echo(f"{left.pgclass.name} is a child node")

    click.echo(f"Will process {len(pairs)} pairs")
    with Pool(n_threads) as pool:
        results = pool.imap_unordered(merging.merge_working_pair, pairs)
        for oid in results:
            if oid is not None and isinstance(oid, int):
                with ctx.obj["dbconf"] as db:
                    relname = db.query(PGClass).get(oid).relname
                    logger.success(relname)


@partitioning.command()
@click.pass_context
@click.argument("model", type=QLPModelType())
def attach_orphaned_partitions(ctx, model):
    with ctx.obj["dbconf"] as db:
        working_group = (
            db.query(RangedPartitionTrack)
            .filter(RangedPartitionTrack.same_model(model))
            .all()
        )
        orphans = []
        for track in tqdm(working_group):
            if track.pgclass is None:
                logger.error(
                    f"BAD TRACK {track.min_range} to {track.max_range}"
                )
                continue
            if len(track.pgclass.parent) == 0:
                logger.warning(f"{track.pgclass.relname} is orphaned!")
                orphans.append(track)

        parent = model.__tablename__
        if not orphans:
            logger.success("No orphaned tables.")
            return 0
        for orphan in tqdm(orphans, unit="orphans"):
            merging.attach(db, parent, orphan.pgclass, orphan)
            logger.info(f"Attached {orphan.pgclass.name}")
        db.commit()
        logger.success(f"Attached {len(orphans)} orphaned tables")
