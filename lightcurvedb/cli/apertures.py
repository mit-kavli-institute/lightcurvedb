import click

from lightcurvedb.cli.base import lcdbcli
from lightcurvedb.models import Aperture


@lcdbcli.command()
@click.pass_context
@click.argument("name", type=str)
@click.argument("aperture-string", type=str)
def add_aperture(ctx, name, aperture_string):
    """
    Add an aperture definition to the database
    """
    with ctx.obj["dbfactory"]() as db:
        check = (
            db.session.query(Aperture)
            .filter(Aperture.name == name)
            .one_or_none()
        )

        s_r, i_r, o_r = Aperture.from_aperture_string(aperture_string)

        if check:
            # Update
            click.echo(click.style("Updating {0}".format(check), fg="yellow"))
            value = click.prompt(
                "Enter a new name (empty input is considered to be no change)",
                type=str,
                default=check.name,
            )
            if value:
                check.name = value
        else:
            # Inserting
            aperture = Aperture(
                name=name, star_radius=s_r, inner_radius=i_r, outer_radius=o_r
            )
            click.echo(
                click.style(
                    "Creating aperture {0}".format(aperture), fg="green"
                )
            )

        if not ctx.obj["dryrun"]:
            if check:
                click.echo(
                    click.style("Will update {0}".format(check), fg="yellow")
                )
            else:
                click.echo(
                    click.style("Will insert {0}".format(aperture), fg="green")
                )
                db.add(aperture)
            prompt = click.style("Do these changes look ok?", bold=True)
            click.confirm(prompt, abort=True)
            db.commit()
        else:
            db.rollback()
