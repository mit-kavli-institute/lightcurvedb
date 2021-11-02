from __future__ import division, print_function

import os
from glob import glob
from itertools import product

import click
import pandas as pd

from lightcurvedb import db
from lightcurvedb.cli.base import lcdbcli
from lightcurvedb.cli.types import CommaList
from lightcurvedb.core.ingestors.cache import IngestionCache
from lightcurvedb.core.ingestors.temp_table import (
    FileObservation,
    TIC8Parameters,
)
from lightcurvedb.core.tic8 import TIC8_DB
from lightcurvedb.models import Orbit
from lightcurvedb.util.contexts import extract_pdo_path_context
from tabulate import tabulate
from tqdm import tqdm


def catalog_df(*catalog_files):
    dfs = []
    for csv in catalog_files:
        df = pd.read_csv(
            csv,
            delim_whitespace=True,
            names=[
                "tic_id",
                "right_ascension",
                "declination",
                "tmag",
                "x",
                "y",
                "z",
                "q",
                "k",
            ],
        )
        dfs.append(df)
    dfs = pd.concat(dfs)
    dfs = dfs.set_index("tic_id")[["right_ascension", "declination", "tmag"]]
    dfs = dfs[~dfs.index.duplicated(keep="last")]
    return dfs


@lcdbcli.group()
@click.pass_context
def cache(ctx):
    """
    Commands for interacting with local disk-cache.
    """
    pass


@cache.command()
@click.pass_context
@click.argument("orbits", type=int, nargs=-1)
@click.option("--force-tic8-query", is_flag=True)
def load_stellar_param(ctx, orbits, force_tic8_query):
    tic8 = TIC8_DB().open() if force_tic8_query else None
    observed_tics = set()
    with ctx.obj["dbconf"] as db, IngestionCache() as cache:
        for orbit_number in orbits:
            orbit = (
                db.query(Orbit)
                .filter(Orbit.orbit_number == orbit_number)
                .one()
            )
            distinct_tic_q = (
                cache.session.query(FileObservation.tic_id)
                .filter(FileObservation.orbit_number == orbit_number)
                .distinct()
                .all()
            )
            obs_tics = {r for r, in distinct_tic_q}
            for tic in obs_tics:
                observed_tics.add(tic)
            if force_tic8_query:
                q = tic8.mass_stellar_param_q(
                    obs_tics,
                    "id",
                    "ra",
                    "dec",
                    "tmag",
                )

                tic_params = pd.read_sql(
                    q.statement, tic8.bind, index_col=["id"]
                )
            else:
                run_dir = orbit.get_qlp_directory(suffixes=["ffi", "run"])
                click.echo("Looking for catalogs in {0}".format(run_dir))
                catalogs = glob(os.path.join(run_dir, "catalog*full.txt"))
                tic_params = catalog_df(*catalogs)
        if tic8:
            tic8.close()

        click.echo("Processing")
        click.echo(tic_params)

        click.echo("Determining what needs to be updated in cache")
        params = []
        tic_params.reset_index(inplace=True)
        for kw in tic_params.to_dict("records"):
            check = (
                cache.session.query(TIC8Parameters)
                .filter(TIC8Parameters.tic_id == kw["tic_id"])
                .one_or_none()
            )
            if check:
                continue
            param = TIC8Parameters(**kw)
            params.append(param)

        click.echo("Updating {0} entries".format(len(params)))
        cache.session.add_all(params)

        if not ctx.obj["dryrun"]:
            cache.commit()
        else:
            cache.session.rollback()

        click.echo("Added {0} definitions".format(len(params)))
    click.echo("Done")


@cache.command()
@click.pass_context
def get_missing_stellar_param(ctx):
    with IngestionCache() as cache, TIC8_DB() as tic8:
        # grab observed tics
        click.echo("querying cache for TIC definitions")
        obs_tics = {
            tic for tic, in cache.session.query(FileObservation.tic_id)
        }
        tics_w_param = {
            tic for tic, in cache.session.query(TIC8Parameters.tic_id)
        }

        missing = obs_tics - tics_w_param
        if len(missing) == 0:
            click.echo(
                click.style(
                    "No TICs in the cache are without TIC8 parameters",
                    fg="green",
                )
            )
            return 0
        else:
            click.echo(
                "Resolving {0} TICs from TIC8".format(
                    click.style(str(len(missing)), bold=True)
                )
            )

        q = tic8.mass_stellar_param_q(missing, "id", "ra", "dec", "tmag")

        insert_q = TIC8Parameters.__table__.insert().values(q)

        if not ctx.obj["dryrun"]:
            cache.session.execute(insert_q)
            cache.session.commit()
            click.echo(
                "Updated {0} TICs without stellar parameters".format(
                    click.style(str(len(missing)), fg="green", bold=True)
                )
            )
        else:
            "Would update {0} TICs".format(
                click.style(str(len(missing)), fg="yellow", bold=True)
            )


@cache.command()
@click.pass_context
@click.argument("param-csv", type=click.Path(dir_okay=False, exists=True))
def stellar_params_from_file(ctx, param_csv):
    file_params = pd.read_csv(param_csv)
    file_tics = set(file_params.tic_id)
    with IngestionCache() as cache:
        cache_tics = {
            tic
            for tic, in cache.session.query(TIC8Parameters.tic_id).distinct()
        }

        missing = file_tics - cache_tics
        new_params = file_params[file_params.tic_id.isin(missing)]
        cache.session.bulk_insert_mappings(
            TIC8Parameters, new_params.to_dict("records")
        )
        if ctx.obj["dryrun"]:
            cache.session.rollback()
            click.echo(new_params)
            click.echo(
                "Would insert {0} new stellar parameter rows".format(
                    click.style(str(len(new_params)), bold=True, fg="yellow")
                )
            )
        else:
            cache.session.commit()
            click.echo(
                "Committed {0} new stellar parameter rows".format(
                    click.style(str(len(new_params)), bold=True, fg="green")
                )
            )


@cache.command()
@click.pass_context
@click.argument(
    "LC_paths", nargs=-1, type=click.Path(file_okay=False, exists=True)
)
def update_file_cache(ctx, lc_paths):
    needed_contexts = {"camera", "ccd", "orbit_number"}
    with IngestionCache() as cache:
        for path in lc_paths:
            context = extract_pdo_path_context(path)
            if not all(x in context for x in needed_contexts):
                click.echo(
                    "Could not find needed contexts for path "
                    "{0} found: {1}".format(path, context)
                )
                continue
            h5s = glob(os.path.join(path, "*.h5"))
            existing_files = (
                cache.session.query(FileObservation)
                .filter(
                    FileObservation.camera == context["camera"],
                    FileObservation.ccd == context["ccd"],
                    FileObservation.orbit_number == context["orbit_number"],
                )
                .all()
            )
            existing_file_map = {
                (fo.tic_id, fo.camera, fo.ccd, fo.orbit_number): fo.id
                for fo in existing_files
            }

            to_add = []
            for h5 in h5s:
                tic_id = int(os.path.basename(h5).split(".")[0])
                key = (
                    tic_id,
                    int(context["camera"]),
                    int(context["ccd"]),
                    int(context["orbit_number"]),
                )
                check = existing_file_map.get(key, None)
                if not check:
                    check = FileObservation(
                        tic_id=tic_id, file_path=h5, **context
                    )
                    to_add.append(check)
            cache.session.add_all(to_add)
            click.echo("Added {0} new FileObservations".format(len(to_add)))
        cache.commit()


@cache.command()
@click.pass_context
@click.argument("orbits", type=int, nargs=-1)
@click.option("--cameras", type=CommaList(int), default="1,2,3,4")
@click.option("--ccds", type=CommaList(int), default="1,2,3,4")
def quality_flags(ctx, orbits, cameras, ccds):
    with db, IngestionCache() as cache:
        click.echo("Preparing cache and querying for orbits")
        orbits = db.orbits.filter(Orbit.orbit_number.in_(orbits)).order_by(
            Orbit.orbit_number.asc()
        )

        qflag_dfs = []

        for orbit, camera, ccd in product(orbits, cameras, ccds):
            expected_qflag_name = "cam{0}ccd{1}_qflag.txt".format(camera, ccd)
            full_path = os.path.join(
                orbit.get_qlp_run_directory(), expected_qflag_name
            )

            click.echo("Parsing {0}".format(full_path))
            df = pd.read_csv(
                full_path,
                delimiter=" ",
                names=["cadences", "quality_flags"],
                dtype={"cadences": int, "quality_flags": int},
            )
            df["cameras"] = camera
            df["ccds"] = ccd
            qflag_dfs.append(df)

        qflags = pd.concat(qflag_dfs)
        qflags = qflags.set_index(["cadences", "cameras", "ccds"])
        qflags = qflags[~qflags.index.duplicated(keep="last")]
        click.echo(
            click.style("==== Will Process ====", fg="green", bold=True)
        )
        click.echo(qflags)

        click.echo("Sending to cache...")
        updated, inserted = cache.consolidate_quality_flags(qflags)
        click.echo(
            "Updating {0} and inserting {1} quality_flags".format(
                click.style(str(updated), bold=True, fg="yellow"),
                click.style(str(inserted), bold=True, fg="green"),
            )
        )
        cache.commit()
        click.echo("Done")


@cache.command()
@click.pass_context
@click.argument("tic", type=int)
def list_files_for(ctx, tic):
    with IngestionCache() as cache:
        q = cache.session.query(
            FileObservation.camera,
            FileObservation.ccd,
            FileObservation.orbit_number,
            FileObservation.file_path,
        ).filter(FileObservation.tic_id == tic)

        click.echo(
            tabulate(q, headers=["camera", "ccd", "orbit_number", "file_path"])
        )


@cache.command()
@click.pass_context
@click.argument("orbit_number", type=int)
@click.argument("camera", type=int)
@click.argument("ccd", type=int)
@click.argument("lc_path", type=click.Path(exists=True, file_okay=False))
def refresh_file_cache(ctx, orbit_number, camera, ccd, lc_path):
    with IngestionCache() as cache:
        q = cache.session.query(FileObservation).filter(
            FileObservation.orbit_number == orbit_number,
            FileObservation.camera == camera,
            FileObservation.ccd == ccd,
        )

        context = extract_pdo_path_context(lc_path)
        if orbit_number != int(context["orbit_number"]):
            click.echo(
                click.style(
                    "Orbit numbers in path and in arguments do not match!",
                    fg="red",
                )
            )
            return 1
        if camera != int(context["camera"]):
            click.echo(
                click.style(
                    "Camera in path and in arguments do not match!", fg="red"
                )
            )
            return 1
        if ccd != int(context["ccd"]):
            click.echo(
                click.style(
                    "ccd in path and in arguments do not match!", fg="red"
                )
            )
            return 1

        h5s = glob(os.path.join(lc_path, "*.h5"))
        all_obs = []
        for h5 in tqdm(h5s, unit=" files"):
            tic_id = int(os.path.basename(h5).split(".")[0])
            key = (
                tic_id,
                int(context["camera"]),
                int(context["ccd"]),
                int(context["orbit_number"]),
            )
            obs = FileObservation(tic_id=tic_id, file_path=h5, **context)
            all_obs.append(obs)
        click.echo(f"Deleting {q.count()} file observations from cache")
        q.delete(synchronize_session=False)
        click.echo(f"Adding {len(all_obs)} file observations as replacement")
        cache.session.add_all(all_obs)
        cache.commit()
