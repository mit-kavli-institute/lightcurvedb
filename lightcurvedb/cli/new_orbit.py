import click
import os
import re
import sys
from functools import partial
from lightcurvedb.models import FrameType, Frame, Orbit
from multiprocessing import Pool
from .base import lcdbcli


if sys.version_info[0] >= 3:
    from pathlib import Path

    def get_parent_dir(path):
        return Path(path).parts[-1]
else:
    def get_parent_dir(path):
        return os.path.basename(
            os.path.abspath(
                os.path.join(
                    path,
                    os.pardir
                )
            )
        )


FITS_CHECK = re.compile(
    r'tess\d+-\d+-[1-4]-crm-ffi\.fits$'
)


TYPE_MAP = {
    'ffi_fits': 'Raw FFI',
    'cal_ffi_fits': 'TICA Calibrated FFI',
    'sub': 'Median Subtracted Frames'
}


ORBIT_EXTR = re.compile(
    r'orbit-(?P<orbit_number>[0-9]+)'
)
CAM_EXTR = re.compile(
    r'cam(?P<camera>[1-4])'
)
CCD_EXTR = re.compile(
    r'ccd(?P<ccd>[1-4])'
)


def ingest_directory(ctx, session, path, cadence_type):
    orbit_context = ORBIT_EXTR.search(path)

    files = []

    parent_dir = get_parent_dir(path)
    mapped = TYPE_MAP[parent_dir]
    frame_type = session.query(FrameType).get(mapped)
    if frame_type is None:
        click.echo(
            'Found no definition for frame type {0}, creating '
            'according to specification.'.format(mapped)
        )
        frame_type = FrameType(name=TYPE_MAP[parent_dir])
        click.echo(
            click.style(
                'Generated frametype {0}'.format(frame_type),
                fg='green'
            )
        )
        session.add(frame_type)
        if not ctx.obj['dryrun']:
            session.commit()

    files = os.listdir(path)
    accepted = []
    rejected = []
    for filename in files:
        fullpath = os.path.join(path, filename)
        check = FITS_CHECK.search(fullpath)
        if check:
            accepted.append(fullpath)
        else:
            rejected.append(fullpath)

    click.echo(
        'Found {0} fits files'.format(
            click.style(str(len(accepted)), bold=True, fg='green')
        )
    )
    click.echo(
        'Rejected {0} files'.format(
            click.style(str(len(rejected)), bold=True, fg='red')
        )
    )

    if not orbit_context:
        raise RuntimeError(
            'Could not find an orbit in the path'
        )

    orbit_number = int(orbit_context.groupdict()['orbit_number'])

    # Attempt to find the orbit
    orbit = session.orbits.filter_by(orbit_number=orbit_number).one_or_none()

    if not orbit:
        click.echo('Orbit {0} not found! Will make one'.format(orbit_number))
        sector = int((orbit_number + 1) / 2) - 4

        # sanity checks: see if the entered sector is looks ok
        checks = session.orbits.filter(
            Orbit.sector > sector,
            Orbit.orbit_number < orbit_number
        ).order_by(Orbit.orbit_number).all()

        if len(checks) > 0:
            for sanity_check in checks:
                click.echo(
                    'Orbit {0} '
                    'has a smaller sector ID {1}'.format(
                        sanity_check.orbit_number,
                        sanity_check.sector
                    )
                )
            click.confirm('Are you sure this is OK?', abort=True)
        orbit = Orbit.generate_from_fits(accepted)
        orbit.sector = sector
        click.echo(
            click.style('Generated {0}'.format(orbit), fg='green')
        )
        session.add(orbit)
        if not ctx.obj['dryrun']:
            session.commit()

    func = partial(Frame.from_fits, cadence_type=cadence_type)
    with Pool() as p:
        click.echo('Generating frames')
        frames = p.map(func, accepted)
        for frame in frames:
            frame.orbit = orbit
            frame.frame_type = frame_type

    click.echo(
        'Generated {0} frames from {1} files'.format(
            len(frames),
            len(accepted)
        )
    )
    return frames


@lcdbcli.command()
@click.pass_context
@click.argument('ingest_directories', nargs=-1)
@click.option('--new-orbit/--no-new-orbit', default=False)
@click.option('--cadence-type', default=30, type=int)
def ingest_frames(ctx, ingest_directories, new_orbit, cadence_type):
    with ctx.obj['dbconf'] as db:
        added_frames = []
        for directory in ingest_directories:
            frames = ingest_directory(ctx, db, directory, cadence_type)
            db.session.add_all(frames)
            added_frames += frames

        if ctx.obj['dryrun']:
            db.session.rollback()
            click.echo(
                click.style(
                    'Dryrun! Rolling back {0} frames!'.format(
                        len(added_frames)
                    ),
                    fg='yellow',
                    bold=True
                )
            )
        else:
            db.session.commit()
            click.echo(
                click.style(
                    'Committed {0} frames!'.format(len(added_frames)),
                    fg='green',
                    bold=True
                )
            )
