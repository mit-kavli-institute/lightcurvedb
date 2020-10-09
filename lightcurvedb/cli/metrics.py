import click
from tabulate import tabulate
from sqlalchemy import or_
from lightcurvedb.cli.base import lcdbcli
from lightcurvedb.models.metrics import QLPProcess, QLPAlteration


@lcdbcli.group()
@click.pass_context
def metrics(ctx):
    """Metric commands"""
    pass


@metrics.command()
@click.pass_context
@click.option("--job_type", type=str)
def processes(ctx, job_type):
    """List the defined QLPProcesses"""
    with ctx.obj["dbconf"] as db:
        q = db.query(QLPProcess)

        if job_type:
            q = q.filter(QLPProcess.job_type == job_type)
        table = tabulate([r.to_dict for r in q.all()], headers="keys")
        click.echo(table)


@metrics.command()
@click.pass_context
@click.argument("job_type", type=str)
@click.option("--alteration_type", type=str)
@click.option(
    "--after",
    type=click.DateTime(),
    help="Only return results after this time",
)
@click.option(
    "--before",
    type=click.DateTime(),
    help="Only return results before this time",
)
@click.option(
    "--model",
    "-m",
    type=str,
    multiple=True,
    help="Filter for models use ILIKE",
)
def alterations(ctx, job_type, alteration_type, after, before, model):
    """Tabulate the QLPAlterations that have been performed"""
    with ctx.obj["dbconf"] as db:
        q = (
            db.query(QLPAlteration)
            .join(QLPAlteration.process)
            .filter(QLPProcess.job_type == job_type)
        )
        if alteration_type:
            q = q.filter(
                QLPAlteration.alteration_type == alteration_type.lower()
            )
        if before and after:
            # Get results where date ranges overlap
            q = q.filter(
                or_(
                    QLPAlteration.date_during_job(before),
                    QLPAlteration.date_during_job(after),
                )
            )
        elif before:
            q = q.filter(QLPAlteration.date_during_job(before))
        elif after:
            q = q.filter(QLPAlteration.date_during_job(after))
        if len(model) > 0:
            conditions = tuple(
                QLPAlteration.target_model.ilike("%{0}%".format(m))
                for m in model
            )
            if len(conditions) > 1:
                q = q.filter(or_(*conditions))
            else:
                q = q.filter(conditions[0])

        table = tabulate([r.to_dict for r in q.all()], headers="keys")
        click.echo(table)
