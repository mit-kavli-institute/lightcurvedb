from __future__ import division, print_function

import click
from lightcurvedb import model as defined_models


@lcdbcli.group()
@click.pass_context
def partitioning(ctx):
    if ctx['dryrun']:
        click.echo(
            click.style(
                'Running in dryrun mode. No partitions will altered!',
                fg='green',
                bold=True
            )
        )


@partitioning.command()
@click.argument('model', type=str)
@click.argument('number_of_new_partitions', type=click.IntRange(min_value=1))
@click.argument('blocksize', type=click.IntRange(min_value=1))
def create_partitions(ctx, model, number_of_new_partitions, blocksize):
    """
    Create ranged partitions on the given MODEL with ranges equivalent to the
    BLOCKSIZE.
    """
    with ctx.obj['dbconf'] as db:
        # Get current partition of the table.
        target_model = getattr(defined_models, model)
        partitions = db.get_partition_df(target_model)
