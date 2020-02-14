import click
import os
import itertools
from sqlalchemy.dialects.postgresql import insert
from collections import defaultdict
from multiprocessing import Pool, cpu_count
from lightcurvedb.models import Aperture, Orbit, LightcurveType, Lightcurve, Lightpoint
from lightcurvedb.core.ingestors.lightcurve_ingestors import h5_to_matrices
from lightcurvedb.util.logging import make_logger
from lightcurvedb.util.iter import chunkify
from lightcurvedb.core.connection import db_from_config
from glob import glob
from h5py import File as H5File
from .base import lcdbcli
from .utils import find_h5
from .types import CommaList

logger = make_logger('H5 Ingestor')

@lcdbcli.group()
@click.pass_context
def lightcurve(ctx):
    pass


def determine_orbit_path(orbit_dir, orbit, cam, ccd):
    orbit_name = 'orbit-{}'.format(orbit)
    cam_name = 'cam{}'.format(cam)
    ccd_name = 'ccd{}'.format(ccd)
    return os.path.join(orbit_dir, orbit_name, 'ffi', cam_name, ccd_name, 'LC')


def yield_raw_h5(filepath):
    results = []
    for h5_result in h5_to_matrices(filepath):
        results.append(h5_result)
    return results


def extr_tic(filepath):
    return int(os.path.basename(filepath).split('.')[0])


def map_new_lightcurves(new_lc):
    cadence_type, lc_type, aperture, tic = new_lc
    return Lightcurve(
        cadence_type=cadence_type,
        lightcurve_type_id=lc_type,
        aperture_id=aperture,
        tic_id=tic
    )


def expand(x):
    return (
        (
            x.cadence_type,
            x.lightcurve_type_id,
            x.aperture_id,
            x.tic_id
        ),
        x
    )

def lightpoint_dict(T, lc_id):
    return {
        'cadence': T[0],
        'barycentric_julian_date': T[1],
        'value': T[2],
        'error': T[3],
        'x_centroid': T[4],
        'y_centroid': T[5],
        'quality_flag': T[6],
        'lightcurve_id': lc_id
    }


def yield_lightpoints(collection, lc_id_map, merging=False):
    if not merging:
        for key, lcs in collection.items():
            lc_id = lc_id_map[key]
            for lc in lcs:
                for lp in lc['data'].T:
                    yield lightpoint_dict(lp, lc_id)
    else:
        for lc_id, lcs in collection.items():
            for lc in lcs:
                for lp in lc.T:
                    yield lightpoint_dict(lp, lc_id)


def insert_lightcurves(config, lightcurves):
    with db_from_config(config) as db:
        for lc in lightcurves:
            if lc is None:
                continue
            db.add(lc)
        db.commit()
    return lightcurves

def make_merge_stmt(points):
    q = insert(Lightcurve).values(
        points
    ).on_confict_do_update(
        constraint='lc_cadence_unique'
    )
    return q;


@lightcurve.command()
@click.pass_context
@click.argument('orbits', type=int, nargs=-1)
@click.option('--n-process', type=int, default=-1, help='Number of cores. <= 0 will use all cores available')
@click.option('--cameras', type=CommaList(int), default='1,2,3,4')
@click.option('--ccds', type=CommaList(int), default='1,2,3,4')
@click.option('--orbit-dir', type=click.Path(exists=True, file_okay=False), default='/pdo/qlp-data')
@click.option('--cadence_type', type=int, default=30)
@click.option('--n-lp-insert', type=int, default=10**5)
def ingest_h5(ctx, orbits, n_process, cameras, ccds, orbit_dir, cadence_type, n_lp_insert):
    with ctx.obj['dbconf'] as db:
        orbits = db.orbits.filter(Orbit.orbit_number.in_(orbits)).all()
        orbit_numbers = [o.orbit_number for o in orbits]
        apertures = db.apertures.all()
        lc_types = db.lightcurve_types.filter(
            LightcurveType.name.in_(('KSPMagnitude', 'RawMagnitude'))
        ).all()

        aperture_map = {
            a.name: a.id for a in apertures
        }
        lc_type_map = {
            lt.name: lt.id for lt in lc_types
        }

        click.echo(
            'Ingesting {} orbits with {} apertures'.format(len(orbits), len(apertures))
        )

        if n_process <= 0:
            n_process = cpu_count()

        click.echo(
            'Utilizing {} cores'.format(click.style(str(n_process), bold=True))
        )

        path_iterators = []
        for cam, ccd, orbit in itertools.product(cameras, ccds, orbit_numbers):
            lc_path = determine_orbit_path(orbit_dir, orbit, cam, ccd)
            path_iterators.append(find_h5(lc_path))

        all_files = list(itertools.chain(*path_iterators))
        tics = set()

        # Load defined tics
        click.echo(
            'Extracting defined tics from {} files'.format(str(len(all_files)))
        )
        with Pool(n_process) as p:
            for tic in p.map(extr_tic, all_files):
                tics.add(tic)

        click.echo('Found {} unique tics'.format(len(tics)))

        # Since we allow multi-orbit ingestions keys are not unique
        resultant = defaultdict(list)

        with Pool(n_process) as p:
            with click.progressbar(all_files, label='Reading H5 files') as file_iter:
                for result in p.imap_unordered(yield_raw_h5, file_iter):
                    for raw_lc in result:
                        type_id = lc_type_map[raw_lc['lc_type']]
                        aperture_id = aperture_map[raw_lc['aperture']]
                        tic = raw_lc['tic']
                        key = (cadence_type, type_id, aperture_id, tic)
                        resultant[key].append(raw_lc['data'])

        # Load lightcurves
        existing_lightcurves = db.lightcurves_by_tics(tics).all()

        # Merge
        to_merge = {}
        click.echo('Determining merges for {} lightcurves'.format(len(existing_lightcurves)))

        for lc in existing_lightcurves:
            key = (cadence_type, lc.lightcurve_type_id, lc.aperture_id, lc.tic_id)
            try:
                raw_lcs = resultant.pop(key)
                to_merge[lc.id] = raw_lcs
            except KeyError:
                # Nothing to merge with, ignore
                continue

        # resultant collection now only contains new lightcurves
        click.echo(
            click.style(
                'Will merge {} lightcurves'.format(len(to_merge)),
                fg='yellow',
                bold=True
            )
        )
        click.echo(
            click.style(
                'Will insert {} lightcurves'.format(len(resultant)),
                fg='green',
                bold=True
            )
        )

        if not ctx.obj['dryrun']:
            prompt = click.style('Do these changes look ok?', bold=True)
            click.confirm(prompt, abort=True)
            click.echo('\tBeginning interpretation of new lightcurves')

            with Pool(n_process) as p:
                click.echo('\tMapping new lightcurves')
                new_lightcurves = p.map(map_new_lightcurves, resultant)
                click.echo('\tInserting new lightcurves...')
    
                batch = chunkify(new_lightcurves, 100000)
                config = db._config
                inserted_batches = p.starmap(
                    insert_lightcurves,
                    itertools.product(
                        [config],
                        batch
                    )
                )
                new_lightcurves = itertools.chain(inserted_batches)

                click.echo('\tInserted')

                lc_id_map = {
                    id: value for id, value in p.imap_unordered(
                        expand,
                        new_lightcurves
                    )
                }

            points = []
            click.echo('\tSerializing points')
            to_insert = yield_lightpoints(resultant, lc_id_map, merging=False)
            to_potentially_merge = yield_lightpoints(to_merge, None, merging=True)
            
            click.echo('\tInserting points...')

            for chunk in chunkify(to_insert, n_lp_insert):
                ok_lps = list(filter(lambda x: x, chunk))
                click.echo('\t\tInserting {} points'.format(len(ok_lps)))
                db.session.bulk_insert_mappings(
                    Lightpoint,
                    ok_lps
                )

            click.echo('\tMerging points...')
            for chunk in chunkify(n_lp_insert, to_potentially_merge):
                ok_lps = list(filter(lambda x: x, chunk))
                click.echo('\t\tMerging {} points'.format(len(ok_lps)))
                q = Lightpoint.bulk_upsert_stmt(chunk)
                db.session.execute(q)

            db.session.commit()
        click.echo('Done')
