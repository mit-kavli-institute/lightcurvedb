import click

from lightcurvedb.models.frame import FrameType

from . import lcdbcli


@lcdbcli.command()
@click.pass_context
@click.argument("frametype-name", type=str)
def add_frametype(ctx, frametype_name):
    """
    Add a Frame-Type Definition to the database.
    """
    with ctx.obj["dbconf"] as db:
        # Check if we're updating or inserting
        check = (
            db.session.query(FrameType)
            .filter_by(name=frametype_name)
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
                    "Creating new frame type {0}".format(frametype_name),
                    fg="green",
                )
            )
            desc = click.prompt(
                "Enter a description for {0}".format(frametype_name)
            )
            new_type = FrameType(name=frametype_name, description=desc)

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
