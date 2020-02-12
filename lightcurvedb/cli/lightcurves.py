import click
import os
import itertools
from multiprocessing import Pool, cpu_count
from lightcurvedb.models import Aperture, Orbit
from lightcurvedb.core.ingestors.lightcurve_ingestors import h5_to_matrices
from glob import glob
from h5py import File as H5File
from .base import lcdbcli
from .utils import find_h5
from .types import CommaList


@lcdbcli.group()
@click.pass_context
def lightcurve(ctx):
    pass

def determine_orbit_path(orbit, orbit_dir, cam, ccd):
    orbit_name = 'orbit-{}'.orbit.orbit_number
    cam_name = 'cam{}'.format(cam)
    ccd_name = 'ccd{}'.format(ccd)
    return os.path.join(orbit_dir, orbit_name, 'ffi', cam_name, ccd_name, 'LC')

def yield_raw_h5(filepath):
    return list(h5_to_matricies(filepath))

def extr_tic(filepath):
    return int(os.path.basename(filepath).split('.')[0])


@lightcurve.command()
@click.pass_context
@click.argument('orbits', type=int, nargs=-1)
@click.option('--n-process', type=int, default=-1, help='Number of cores. <= 0 will use all cores available')
@click.option('--cameras', type=CommaList(int), default='1,2,3,4')
@click.option('--ccds', type=CommaList(int), default='1,2,3,4')
@click.option('--orbit-dir', type=click.Path(exists=True, file_okay=False), default='/pdo/qlp-data')
def ingest_h5(ctx, orbits, n_process, cameras, ccds, orbit_dir):
    with ctx.obj['dbconf'] as db:
        orbits = db.orbits.filter(Orbit.orbit_number.in_(orbits)).all()
        orbit_numbers = [o.orbit_number for o in orbits]
        apertures = db.apertures.all()

        click.echo(
            'Ingesting {} orbits with {} apertures'.format(len(orbits), len(apertures))
        )

        if not n_process:
            n_process = cpu_count()

        click.echo(
            'Utilizing {} cores'.format(click.style(n_process, bold=True))
        )

        path_iterators = []
        for cam, ccd, orbit in itertools.product(cameras, ccds, orbits):
            lc_path = determine_orbit_path(orbit_dir, orbit, cam, ccd)
            path_iterators.append(find_h5(lc_path))

        all_files = list(itertools.chain(*path_iterators))
        tics = set()

        # Load defined tics
        click.echo('Extracting defined tics')
        with Pool(n_process) as p:
            for tic in p.map(extr_tic, all_files):
                tics.add(tic)
        click.echo('Found {} unique tics'.format(len(tics)))

        with Pool(n_process) as p:
            pass
