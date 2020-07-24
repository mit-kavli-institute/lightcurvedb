import click
from tabulate import tabulate
from sqlalchemy.func import between, _or
from lightcurvedb.cli.base import lcdbcli
from lightcurvedb.models.metrics import QLPProcess, QLPAlteration


@lcdbcli.group()
@click.pass_context
def metrics(ctx):
    """Base metric class"""
    pass

@metrics.command
@click.pass_context
@click.option('--job_type', type=str)
def processes(ctx, job_type):
    with ctx['dbconf'] as db:
        q = db.query(QLPProcess).filter(
            QLPProcess.job_type == job_type
        )
        table = tabulate(
            [r.to_dict for r in q.all()],
            headers='keys'
        )
        click.echo(table)


@metrics.command
@click.pass_context
@click.argument('job_type', type=str)
@click.option('--alteration_type', type=str)
@click.option('--after', type=click.DateTime, help='Only return results after this time')
@click.option('--before', type=click.DateTime, help='Only return results before this time')
@click.option('--model', '-m', type=str, multiple=True, help='Filter for models use ILIKE')
def alterations(ctx, job_type, alteration_type, after, before, model):
    with ctx['dbconf'] as db:
        q = db.query(QLPAlteration)\
                .join(QLPAlteration.process)\
                .filter(
                    QLPProcess.job_type == job_type
                )
        if alteration_type:
            q = q.filter(
                QLPAlteration.alteration_type == alteration_type.lower()
            )
        if before and after:
            # Get results where date ranges overlap
            q = q.filter(
                _or(
                    between(
                        before,
                        QLPAlteration.time_start,
                        QLPAlteration.time_end
                    ),
                    between(
                        after,
                        QLPAlteration.time_start,
                        QLPAlteration.time_end
                    )
                )
            )
        elif before:
            q = q.filter(
                between(
                    before,
                    QLPAlteration.time_start,
                    QLPAlteration.time_end
                )
            )
        elif after:
            q = q.filter(
                between(
                    after,
                    QLPAlteration.time_start,
                    QLPAlteration.time_end
                )
            )
        if len(model) > 0:
            conditions = (
                QLPAlteration.target_model.ilike(m) for m in model
            )
            q = q.filter(
                _or(*conditions)
            )

        table = tabulate(
            [r.to_dict for r in q.all()],
            headers='keys'
        )
        click.echo(table)
