import click
import os
from lightcurvedb.models import Aperture, Orbit
from lightcurvedb.core.ingestors.lightcurve_ingestors import LightcurveH5Ingestor
from glob import glob
from h5py import File as H5File
from .base import lcdbcli
from .utils import find_h5


@lcdbcli.group()
@click.pass_context
def lightcurve(ctx):
    pass

@lightcurve.command()
@click.pass_context
@click.argument('orbit', type=int)
@click.argument('path', type=click.Path(file_okay=False, exists=True))
def ingest_h5(ctx, orbit, path):
    with ctx.obj['dbconf'] as db:
        target_orbit = db.orbits.filter_by(orbit_number=orbit).one()
        apertures = None
        lc_files = list(find_h5(path))
        ingestor = LightcurveH5Ingestor(context_kwargs={
                'db': db
        })
        found_lightcurves = {}
        with click.progressbar(lc_files) as files:
            for h5 in files:
                for lc in ingestor.ingest(h5, mode='rb'):
                    found_lightcurves[lc.tic_id] = lc

        click.echo(
            click.style(
                'Found {} lightcurves'.format(len(found_lightcurves)),
                bold=True
            )
        )

        # Merge with existing lightcurves
        tics = found_lightcurves.keys()
        mq = db.lightcurves_by_tics(tics)
        for lc_batch in mq.yield_per(1000):
            for lc in lc_batch:
                to_merge = found_lightcurves.pop(lc.tic_id)

