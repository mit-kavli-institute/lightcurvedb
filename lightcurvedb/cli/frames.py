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
@click.argument('frame-subdir', type=str)
@click.argument('orbits', nargs=-1, type=int)
@click.option('--frame-type', default='raw-poc', type=str)
@click.option('--allow-compressed', is_flag=True)
@click.option('--base-dir', default='/pdo/poc-data/orbits/', type=click.Path(exists=True, file_okay=False))
@click.option('--orbit-dir-prefix', default='orbit-', type=str)
def ingest_frames_by_orbit(ctx, frame_subdir, orbits, frame_type, allow_compressed, base_dir, orbit_dir_prefix):
    with ctx.obj['dbconf'] as db:
        # Check that the specified frame type and orbit exists
        frame_type = db.session.query(FrameType).filter_by(name=frame_type).one()

        to_add = []
        to_update = []
        for orbit in orbits:
            orbit = db.session.query(Orbit).filter_by(orbit_number=orbit).one()
            ingestor = FrameIngestor(
                    context_kwargs={
                        'orbit': orbit,
                        'frame_type': frame_type
                        }
                    )
            data_dir = os.path.join(
                base_dir,
                '{}{}'.format(orbit_dir_prefix, orbit.orbit_number),
                frame_subdir
            )
            fits = find_fits(data_dir, allow_compression=allow_compression)
            for frame in ingestor.ingest(fits):
                check = db.session.query(Frame).filter_by(
                    orbit_id=orbit.id,
                    frame_type_id=frame_type.id,
                    cadence=frame.cadence).one_or_none()
                if check:
                    # Update
                    to_update.append(frame)
                else:
                    to_add.append(frame)

        if len(to_add) > 0:
            click.echo(click.style('--Will Add--', bold=True, fg='green'))
            for frame in to_add:
                click.echo(frame)

        if len(to_update) > 0:
            click.echo(click.style('--Will Update--', bold=True, fg='yellow'))
            for frame in to_update:
                click.echo(frame)

        if not ctx.obj['dryrun']:
            # Commit changes to the database
            prompt = click.style('Do these changes look ok?', bold=True)
            click.confirm(prompt, abort=True)
            db.commit()
