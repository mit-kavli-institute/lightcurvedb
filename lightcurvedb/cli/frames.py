import os
from glob import glob

import click
from loguru import logger
from tqdm import tqdm

from lightcurvedb.cli.base import lcdbcli
from lightcurvedb.core.ingestors.frame_ingestor import from_fits
from lightcurvedb.models import Orbit
from lightcurvedb.models.frame import Frame, FrameType
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


@frames.group()
@click.pass_context
@click.argument("frame_path", type=click.Path(file_okay=False, exists=True))
def ingest_frames(ctx, frame_path):
    contextual_paths = {}
    contextual_paths[frame_path] = extract_pdo_path_context(frame_path)

    ctx.obj["frame_paths"] = contextual_paths


@ingest_frames.command()
@click.pass_context
def tica(ctx):
    with ctx.obj["dbconf"] as db:
        tica_type = (
            db.query(FrameType).filter_by(name="TICA Calibrated FFI").one()
        )
        for path, context in ctx.obj["frame_paths"].items():
            orbit_number = int(context["orbit_number"])
            orbit = db.query(Orbit).filter_by(orbit_number=orbit_number).one()
            found_fits = glob(os.path.join(path, "*.fits"))
            logger.debug(f"Processing {len(found_fits)} fits files at {path}")
            for fits in tqdm(found_fits, unit=" files"):
                frame = from_fits(fits, None, tica_type, orbit)
                check = (
                    db.query(Frame)
                    .filter(
                        Frame.frame_type_id == tica_type.name,
                        Frame.orbit_id == orbit.id,
                        Frame.cadence == frame.cadence,
                        Frame.camera == frame.camera,
                        Frame.ccd == frame.ccd,
                    )
                    .one_or_none()
                )
                if check is None:
                    db.add(frame)

            db.commit()
            logger.success(f"Added {len(found_fits)} frames")
