from __future__ import division, print_function

import click
from sys import exit
import lightcurvedb.models as defined_models
from lightcurvedb.core.partitioning import emit_ranged_partition_ddl
from lightcurvedb.cli.base import lcdbcli


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
        current_max = max(partitions['end_range'])

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

        original_begin = max(partitions['end_range'])
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
                'Committed {} new partitions!'.format(len(new_partition_models))
            )
