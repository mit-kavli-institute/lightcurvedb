import pathlib

import click

from lightcurvedb import db_from_config
from lightcurvedb.cli.base import lcdbcli
from lightcurvedb.core.ingestors import camera_quaternions, frames
from lightcurvedb.models import FrameType


@lcdbcli.command()
@click.pass_context
@click.argument("ingest_directories", type=pathlib.Path, nargs=-1)
@click.option("--frame-type-name", type=str, default="Raw FFI")
@click.option("--ffi-subdir", type=pathlib.Path, default="ffi_fits")
@click.option("--quaternion-subdir", type=pathlib.Path, default="hk")
def ingest_frames(
    ctx,
    ingest_directories: list[pathlib.Path],
    frame_type_name: str,
    ffi_subdir: pathlib.Path,
    quaternion_subdir: pathlib.Path,
):
    with db_from_config(ctx.obj["dbconf"]) as db:
        frame_type = (
            db.query(FrameType).filter_by(name=frame_type_name).one_or_none()
        )
        if frame_type is None:
            defined_names = [
                name
                for name, in db.query(FrameType.name).order_by(FrameType.name)
            ]
            defined_names = ", ".join(defined_names)
            raise click.Abort(
                f"Could not determine frame type '{frame_type_name}'. "
                f"Defined frame types are: {defined_names}"
            )

        for orbit_directory in ingest_directories:
            # Ingest Camera Quaternion Files
            camera_quaternions.ingest_directory(
                db, orbit_directory / quaternion_subdir, "*quat.txt"
            )

            # Ingest FITS frames
            frames.ingest_directory(
                db, frame_type, orbit_directory / ffi_subdir, "*.fits"
            )
        if not ctx.obj["dryrun"]:
            db.commit()
        else:
            click.echo("Dryrun")
            db.rollback()
