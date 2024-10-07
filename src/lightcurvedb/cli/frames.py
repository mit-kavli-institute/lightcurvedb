import click

from lightcurvedb import db_from_config
from lightcurvedb.cli.base import lcdbcli
from lightcurvedb.models.frame import FrameType
from lightcurvedb.util.contexts import extract_pdo_path_context


@lcdbcli.group()
def frames():
    pass


@frames.command()
@click.pass_context
@click.argument("frametype-name", type=str)
def add_frametype(ctx, frametype_name):
    """
    Add a Frame-Type Definition to the database.
    """
    with db_from_config(ctx.obj["dbconf"]) as db:
        # Check if we're updating or inserting
        check = (
            db.query(FrameType).filter_by(name=frametype_name).one_or_none()
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


@frames.group()
@click.pass_context
@click.argument("frame_path", type=click.Path(file_okay=False, exists=True))
def ingest_frames(ctx, frame_path):
    contextual_paths = {}
    contextual_paths[frame_path] = extract_pdo_path_context(frame_path)

    ctx.obj["frame_paths"] = contextual_paths
