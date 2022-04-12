import os
import re
from functools import partial
from multiprocessing import Pool

import click

from lightcurvedb.core.ingestors.frames import from_fits
from lightcurvedb.models import CameraQuaternion, Frame, FrameType, Orbit
from lightcurvedb.models.camera_quaternion import get_utc_time
from lightcurvedb.util.contexts import get_parent_dir

from .base import lcdbcli

FITS_CHECK = re.compile(r"tess\d+-\d+-[1-4]-crm-ffi\.fits$")
QUAT_CHECK = re.compile(r"cam(?P<camera>[1-4])_quat.txt$")


TYPE_MAP = {
    "ffi_fits": "Raw FFI",
    "cal_ffi_fits": "TICA Calibrated FFI",
    "sub": "Median Subtracted Frames",
}


ORBIT_EXTR = re.compile(r"orbit-(?P<orbit_number>[0-9]+)")
CAM_EXTR = re.compile(r"cam(?P<camera>[1-4])")
CCD_EXTR = re.compile(r"ccd(?P<ccd>[1-4])")


def process_hk_str(line):
    row = line.strip().split()
    gps_time = float(row[0])
    q1 = float(row[1])
    q2 = float(row[2])
    q3 = float(row[3])
    q4 = float(row[4])

    date = get_utc_time(gps_time).datetime
    return {"date": date, "_w": q1, "_x": q2, "_y": q3, "_z": q4}


def ingest_hk(ctx, session, path):
    files = filter(lambda path: QUAT_CHECK.search(path), os.listdir(path))
    expected_cameras = {1, 2, 3, 4}
    skipped = 0
    existing_quaternions = {
        (date, camera): id_
        for id_, date, camera in session.query(
            CameraQuaternion.id, CameraQuaternion.date, CameraQuaternion.camera
        )
    }
    quaternions = []
    click.echo("Processing quaternion files!")
    for hk in files:
        camera = int(QUAT_CHECK.search(hk).groupdict()["camera"])
        click.echo("\tParsing {0}".format(click.style(hk, bold=True)))
        if camera in expected_cameras:
            expected_cameras.remove(camera)
        lines = open(os.path.join(path, hk), "rt").readlines()
        with Pool() as pool:
            results = pool.imap(process_hk_str, lines, chunksize=10000)

            for kwarg in results:
                camera_quat = dict(camera=camera, **kwarg)

                if (camera_quat["date"], camera) in existing_quaternions:
                    skipped += 1
                    continue

                quaternions.append(camera_quat)
    click.echo(
        "Skipping {0} quaternion rows as they appear to "
        "already exist".format(
            click.style(str(skipped), bold=True, fg="yellow")
        )
    )
    if len(expected_cameras) > 0:
        click.echo(
            "Path {0} seems to not define cameras: {1}.".format(
                click.style(path, bold=True),
                click.style(
                    ", ".join(map(str, sorted(expected_cameras))),
                    bold=True,
                    fg="red",
                ),
            )
        )
    return quaternions


def ingest_directory(ctx, session, path):
    orbit_context = ORBIT_EXTR.search(path)
    orbit_number = int(orbit_context.groupdict()["orbit_number"])

    parent_dir = get_parent_dir(path)
    mapped = TYPE_MAP[parent_dir]
    frame_type = session.query(FrameType).get(mapped)
    if frame_type is None:
        click.echo(
            "Found no definition for frame type {0}, creating "
            "according to specification.".format(mapped)
        )
        frame_type = FrameType(name=TYPE_MAP[parent_dir])
        click.echo(
            click.style(
                "Generated frametype {0}".format(frame_type), fg="green"
            )
        )
        session.add(frame_type)
        if not ctx.obj["dryrun"]:
            session.commit()

    files = os.listdir(path)
    existing_files = {
        path
        for path, in session.query(Frame.file_path)
        .join(Frame.orbit)
        .filter(Orbit.orbit_number == orbit_number)
    }
    accepted = []
    rejected = []
    for filename in files:
        fullpath = os.path.join(path, filename)
        check = FITS_CHECK.search(fullpath)
        if check and fullpath not in existing_files:
            accepted.append(fullpath)
        else:
            rejected.append(fullpath)

    click.echo(
        "Found {0} fits files".format(
            click.style(str(len(accepted)), bold=True, fg="green")
        )
    )
    click.echo(
        "Rejected {0} files".format(
            click.style(str(len(rejected)), bold=True, fg="red")
        )
    )

    if not orbit_context:
        raise RuntimeError("Could not find an orbit in the path")
    if not accepted:
        return []

    # Attempt to find the orbit
    orbit = session.orbits.filter_by(orbit_number=orbit_number).one_or_none()

    if not orbit:
        click.echo("Orbit {0} not found! Will make one".format(orbit_number))
        sector = int((orbit_number + 1) / 2) - 4

        # sanity checks: see if the entered sector is looks ok
        checks = (
            session.orbits.filter(
                Orbit.sector > sector, Orbit.orbit_number < orbit_number
            )
            .order_by(Orbit.orbit_number)
            .all()
        )

        if len(checks) > 0:
            for sanity_check in checks:
                click.echo(
                    "Orbit {0} "
                    "has a smaller sector ID {1}".format(
                        sanity_check.orbit_number, sanity_check.sector
                    )
                )
            click.confirm("Are you sure this is OK?", abort=True)
        orbit = Orbit.generate_from_fits(accepted)
        orbit.sector = sector
        click.echo(click.style("Generated {0}".format(orbit), fg="green"))
        session.add(orbit)
        if not ctx.obj["dryrun"]:
            session.commit()

    p = Pool()
    click.echo("Generating frames")
    frames = p.map(from_fits, accepted)
    for frame in frames:
        frame.orbit = orbit
        frame.frame_type = frame_type

    click.echo(
        "Generated {0} frames from {1} files".format(
            len(frames), len(accepted)
        )
    )
    return frames


@lcdbcli.command()
@click.pass_context
@click.argument("ingest_directories", nargs=-1)
@click.option("--new-orbit/--no-new-orbit", default=False)
@click.option("--ffi-subdir", type=str, default="ffi_fits")
@click.option("--quaternion-subdir", type=str, default="hk")
def ingest_frames(
    ctx,
    ingest_directories,
    new_orbit,
    ffi_subdir,
    quaternion_subdir,
):
    with ctx.obj["dbconf"] as db:
        added_frames = []
        added_quaternions = []
        for directory in ingest_directories:
            ffi_path = os.path.join(directory, ffi_subdir)
            hk_path = os.path.join(directory, quaternion_subdir)

            try:
                frames = ingest_directory(ctx, db, ffi_path)
            except FileNotFoundError:
                click.echo(
                    click.style(
                        "Bad FFI Path/structure {0}".format(
                            click.style(ffi_path, bold=True, blink=True)
                        ),
                        fg="red",
                    )
                )
            quaternions = ingest_hk(ctx, db, hk_path)

            db.session.add_all(frames)
            db.session.bulk_insert_mappings(CameraQuaternion, quaternions)
            added_frames += frames
            added_quaternions += quaternions

        if ctx.obj["dryrun"]:
            db.session.rollback()
            click.echo(
                click.style(
                    "Dryrun! Rolling back {0} frames!".format(
                        len(added_frames)
                    ),
                    fg="yellow",
                    bold=True,
                )
            )
            click.echo(
                click.style(
                    "Dryrun! Rolling back {0} quaternions!".format(
                        len(quaternions)
                    ),
                    fg="yellow",
                    bold=True,
                )
            )
        else:
            db.session.commit()
            click.echo(
                click.style(
                    "Committed {0} frames!".format(len(added_frames)),
                    fg="green",
                    bold=True,
                )
            )
            click.echo(
                click.style(
                    "Committed {0} quaternions!".format(len(quaternions)),
                    fg="green",
                    bold=True,
                )
            )
