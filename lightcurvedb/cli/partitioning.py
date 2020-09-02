from __future__ import division, print_function

import click
import multiprocessing as mp
from sys import exit
import lightcurvedb.models as defined_models
from sqlalchemy import text
from lightcurvedb import db_from_config
from lightcurvedb.core.partitioning import emit_ranged_partition_ddl
from lightcurvedb.cli.base import lcdbcli


def mp_execute(db_config, q, **parameters):
    with db_from_config(db_config) as db:
        try:
            db.session.execute(q, **parameters)
            db.commit()
            return True
        except:
            return False


@lcdbcli.group()
@click.pass_context
def partitioning(ctx):
    if ctx.obj['dryrun']:
        click.echo(
            click.style(
                'Running in dryrun mode. No partitions will altered!',
                fg='green',
                bold=True
            )
        )

@partitioning.command()
@click.pass_context
@click.argument('model', type=str)
def list_partitions(ctx, model):
    """
    List the partitions of MODEL
    """
    with ctx.obj['dbconf'] as db:
        # Get current partition of the table.
        try:
            target_model = getattr(defined_models, model)
        except AttributeError:
            click.echo('No known model {}'.format(model))
            exit(1)

        partitions = db.get_partitions_df(target_model)
        click.echo(partitions)
        click.echo(
            'A total of {} partitions!'.format(
                click.style(
                    str(len(partitions)),
                    bold=True
                )
            )
        )


@partitioning.command()
@click.pass_context
@click.argument('model', type=str)
@click.argument('number_of_new_partitions', type=click.IntRange(min=1))
@click.argument('blocksize', type=click.IntRange(min=1))
def create_partitions(ctx, model, number_of_new_partitions, blocksize):
    """
    Create ranged partitions on the given MODEL with ranges equivalent to the
    BLOCKSIZE.
    """
    with ctx.obj['dbconf'] as db:
        # Get current partition of the table.
        try:
            target_model = getattr(defined_models, model)
        except AttributeError:
            click.echo('No known model {}'.format(model))
            exit(1)

        partitions = db.get_partitions_df(target_model)
        if len(partitions) > 0:
            current_max = max(partitions['end_range'])
        else:
            current_max = 0

        if current_max is None:
            click.echo(
                'Model {} has no partitions! Please define a partition rule in the PSQL shell'.format(model)
            )
            exit(1)

        new_partition_models = []

        for _ in range(number_of_new_partitions):
            ddl = emit_ranged_partition_ddl(
                target_model.__tablename__,
                current_max,
                current_max + blocksize
            )
            new_partition_models.append(ddl)

            click.echo(
                '\tWill emit new PARTITION FROM VALUES ({}) TO ({})'.format(
                    current_max,
                    current_max + blocksize
                )
            )
            current_max += blocksize

        try:
            original_begin = max(partitions['end_range'])
        except ValueError:
            original_begin = 0
        click.echo(
            'Will create {} partitions spanning values from {} to {}'.format(
                len(new_partition_models),
                original_begin,
                current_max
            )
        )

        # Determine if blocksize is an anomaly
        blocksizes = partitions['end_range'] - partitions['begin_range']
        stddev = blocksizes.std()
        mean = blocksizes.mean()

        # Check if blocksize looks odd
        strange_blocksize = not (mean - stddev <= blocksize <= mean + stddev)

        if strange_blocksize:
            click.echo(
                click.style(
                    'Odd blocksize!',
                    bold=True,
                    fg='white',
                    bg='red',
                    blink=True
                )
            )
            click.echo(
                'Specified blocksize {} resides outside avg blocksize {} +- {}'.format(
                    blocksize, mean, stddev
                )
            )

        if not ctx.obj['dryrun']:
            click.confirm('Do the following changes look okay?', abort=True)
            for partition in new_partition_models:
                db.session.execute(partition)
                db.commit()
                click.echo(
                    '\tMade {}'.format(partition)
                )
            click.echo(
                'Committed {} new partitions!'.format(len(new_partition_models))
            )


@partitioning.command()
@click.pass_context
@click.argument('model', type=str)
@click.option('--pattern', '-p', type=str, default='.*')
def delete_partitions(ctx, model, pattern):
    with ctx.obj['dbconf'] as db:
        # Get current partition of the table.
        try:
            target_model = getattr(defined_models, model)
        except AttributeError:
            click.echo('No known model {}'.format(model))
            exit(1)
    
        partitions = db.get_partitions_df(target_model)
        if len(partitions) > 0:
            current_max = max(partitions['end_range'])
        else:
            current_max = 0

        if current_max is None:
            click.echo(
                'Model {} has no partitions! Please define a partition rule in the PSQL shell'.format(model)
            )
            exit(1)
        names = partitions['partition_name']
        mask = names.str.contains(pattern)
        names = names[mask]

        click.echo(
            names
        )
        click.echo(
            'Will remove {} partitions!'.format(
                len(names)
            )
        )
        if not ctx.obj['dryrun']:
            click.confirm('Does this look okay?', abort=True)
            for name in names:
                q = text('DROP TABLE {}'.format(name))
                db.session.execute(q)
                db.commit()
                click.echo('\tDeleted {}'.format(name))


@partitioning.command()
@click.pass_context
@click.argument('model', type=str)
@click.option('--pattern', '-p', type=str, default='.*')
def set_unlogged(ctx, model, pattern):
    try:
        target_model = getattr(defined_models, model)
    except AttributeError:
        click.echo('No known model {}'.format(model))
        exit(1)
    with ctx.obj['dbconf'] as db:
        partitions = db.get_partitions_df(target_model)
        tablenames = list(partitions.partition_name)

        for table in tablenames:
            q = text('ALTER TABLE {} SET UNLOGGED'.format(table))
            click.echo('Altering {}'.format(
                click.style(table, bold=True)
            ))
            db.session.execute(
                q
            )
            db.commit()
        click.echo('Altered {} tables! Done'.format(len(tablenames)))


@partitioning.command()
@click.pass_context
@click.argument('model', type=str)
@click.option('--pattern', '-p', type=str, default='.*')
def set_logged(ctx, model, pattern):
    try:
        target_model = getattr(defined_models, model)
    except AttributeError:
        click.echo('No known model {}'.format(model))
        exit(1)

    with ctx.obj['dbconf'] as db:
        partitions = db.get_partitions_df(target_model)
        tablenames = list(partitions.partition_name)

        for table in tablenames:
            q = text('ALTER TABLE {} SET LOGGED'.format(table))
            click.echo('Altering {}'.format(
                click.style(table, bold=True)
            ))
            db.session.execute(
                q
            )
            db.commit()
        click.echo('Altered {} tables! Done'.format(len(tablenames)))
