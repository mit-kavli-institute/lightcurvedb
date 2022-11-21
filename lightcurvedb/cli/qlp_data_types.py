import click

from lightcurvedb.models import LightcurveType

from . import lcdbcli


@lcdbcli.command()
@click.pass_context
@click.argument("lightcurve-type-name", type=str)
def add_lightcurvetype(ctx, lightcurve_type_name):
    """
    Adds a lightcurve-type definition to the database.
    """
    with ctx.obj["db"] as db:
        check = (
            db.session.query(LightcurveType)
            .filter(LightcurveType == lightcurve_type_name)
            .one_or_none()
        )
        if check:
            # Updating
            click.echo(click.style("Updating {0}".format(check), fg="yellow"))
            value = click.prompt(
                "Enter a new name "
                "(empty input is considered to be no change)",
                type=str,
                default=check.name,
            )
            if value:
                check.name = value
            value = click.prompt(
                "Enter a description "
                "(empty input is considered to be no change)",
                type=str,
                default=check.description,
            )
        else:
            # Inserting
            click.echo(
                click.style(
                    "Creating new frame type {0}".format(lightcurve_type_name),
                    fg="green",
                )
            )
            desc = click.prompt(
                "Enter a description for {0}".format(lightcurve_type_name)
            )
            new_type = LightcurveType(
                name=lightcurve_type_name, description=desc
            )

        if not ctx.obj["dryrun"]:
            if check:
                click.echo(
                    click.style("Update on: {0}".format(check), fg="yellow")
                )
            else:
                click.echo(
                    click.style("Inserting {0}".format(new_type), fg="green")
                )
                db.add(new_type)
            prompt = click.style("Do these changes look ok?", bold=True)
            click.confirm(prompt, abort=True)
            db.commit()
        else:
            db.rollback()
