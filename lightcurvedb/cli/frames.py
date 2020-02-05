import click
import os
from collections import defaultdict
from astropy.io import fits
from glob import glob
from lightcurvedb.core.ingestors.frame_ingestor import FrameIngestor
from lightcurvedb.models.orbit import Orbit
from lightcurvedb.models.frame import FrameType, Frame
from .utils import find_fits
from . import lcdbcli

@lcdbcli.command()
@click.pass_context
@click.argument('frametype-name', type=str)
def create_frametype(ctx, frametype_name):
    with ctx.obj['dbconf'] as db:
        # Check if we're updating or inserting
        check = db.session.query(FrameType).filter_by(name=frametype_name).one_or_none()
        if check:
            # Updating
            click.echo(click.style('Updating {}'.format(check), fg='yellow'))
            value = click.prompt(
                'Enter a new name (empty input is considered to be no change)',
                type=str,
                default=check.name)
            if value:
                check.name = value
            value = click.prompt(
                'Enter a description (empty input is considered to be no change)',
                type=str,
                default=check.description)
        else:
            # Inserting
            click.echo(click.style('Creating new frame type {}'.format(frametype_name), fg='green'))
            desc = click.prompt('Enter a description for {}'.format(frametype_name))
            new_type = FrameType(name=frametype_name, description=desc)

        if not ctx.obj['dryrun']:
            if check:
                click.echo(click.style('Update on: {}'.format(check), fg='yellow'))
            else:
                click.echo(click.style('Inserting {}'.format(new_type), fg='green'))
                db.add(new_type)
            prompt = click.style('Do these changes look ok?', bold=True)
            click.confirm(prompt, abort=True)
            db.commit()


@lcdbcli.command()
@click.pass_context
@click.argument('frame-subdir', type=str)
@click.argument('orbits', nargs=-1, type=int)
@click.option('--frame-type', default='raw-poc', type=str)
@click.option('--cadence-type', default=30, type=int)
@click.option('--allow-compressed', is_flag=True)
@click.option('--base-dir', default='/pdo/poc-data/orbits/', type=click.Path(exists=True, file_okay=False))
@click.option('--orbit-dir-prefix', default='orbit-', type=str)
def ingest_frames_by_orbit(ctx, frame_subdir, orbits, frame_type, cadence_type, allow_compressed, base_dir, orbit_dir_prefix):
    with ctx.obj['dbconf'] as db:
        # Check that the specified frame type and orbit exists
        frame_type = db.session.query(FrameType).filter_by(name=frame_type).one()

        to_add = []
        to_update = []
        for orbit in orbits:
            orbit = db.session.query(Orbit).filter_by(orbit_number=orbit).one()
            ingestor = FrameIngestor(
                    context_kwargs={
                        'cadence_type': cadence_type,
                        }
                    )
            data_dir = os.path.join(
                base_dir,
                '{}{}'.format(orbit_dir_prefix, orbit.orbit_number),
                frame_subdir
            )
            click.echo('Looking for frames in {}'.format(data_dir))
            fits_paths = find_fits(data_dir, allow_compressed=allow_compressed)
            with click.progressbar(fits_paths, label='Ingesting frames for {}'.format(orbit)) as paths:
                for frame in ingestor.ingest(paths):
                    check = db.session.query(Frame).filter_by(
                        orbit_id=orbit.id,
                        frame_type_id=frame_type.id,
                        camera=frame.camera,
                        ccd=frame.ccd,
                        cadence_type=frame.cadence_type,
                        cadence=frame.cadence).one_or_none()
                    if check:
                        # Update
                        check.copy(frame)
                        to_update.append(check)
                        db.add(check)
                    else:
                        frame.orbit = orbit
                        frame.frame_type = frame_type
                        to_add.append(frame)
                        db.add(frame)

        if len(to_add) > 0:
            click.echo(click.style('Will add {} frames'.format(len(to_add)), bold=True, fg='green'))

        if len(to_update) > 0:
            click.echo(click.style('Will update {} frames'.format(len(to_update)), bold=True, fg='yellow'))

        if not ctx.obj['dryrun']:
            # Commit changes to the database
            prompt = click.style('Do these changes look ok?', bold=True)
            click.confirm(prompt, abort=True)
            db.commit()
