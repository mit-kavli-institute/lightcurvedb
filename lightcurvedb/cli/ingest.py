"""
Top level ingest cli module.
"""
import pathlib
import tempfile

import click
import pandas as pd
from loguru import logger

from lightcurvedb import models as m
from lightcurvedb.cli.base import lcdbcli
from lightcurvedb.core.connection import db_from_config
from lightcurvedb.core.ingestors import camera_quaternions, contexts
from lightcurvedb.core.ingestors import frames as frame_ingest
from lightcurvedb.core.ingestors import jobs, lightcurve_arrays


@lcdbcli.group()
@click.option("--n-processes", default=16, type=click.IntRange(min=1))
@click.pass_context
def ingest(ctx, n_processes):
    ctx.obj["n_processes"] = n_processes


@ingest.command()
@click.pass_context
@click.argument("orbit_directories", type=pathlib.Path, nargs=-1)
@click.option("--frame-type-name", type=str, default="Raw FFI")
@click.option("--ffi-subdir", type=pathlib.Path, default="ffi_fits")
@click.option("--quaternion-subdir", type=pathlib.Path, default="hk")
def frames(
    ctx,
    orbit_directories: list[pathlib.Path],
    frame_type_name: str,
    ffi_subdir: pathlib.Path,
    quaternion_subdir: pathlib.Path,
):
    with db_from_config(ctx.obj["dbconf"]) as db:
        frame_type = (
            db.query(m.FrameType).filter_by(name=frame_type_name).one_or_none()
        )
        if frame_type is None:
            defined_names = [
                name
                for name, in db.query(m.FrameType.name).order_by(
                    m.FrameType.name
                )
            ]
            defined_names = ", ".join(defined_names)
            raise click.Abort(
                f"Could not determine frame type '{frame_type_name}'. "
                f"Defined frame types are: {defined_names}"
            )

        for orbit_directory in orbit_directories:
            # Ingest Camera Quaternion Files
            camera_quaternions.ingest_directory(
                db, orbit_directory / quaternion_subdir, "*quat.txt"
            )

            # Ingest FITS frames
            frame_ingest.ingest_directory(
                db, frame_type, orbit_directory / ffi_subdir, "*.fits"
            )
        if not ctx.obj["dryrun"]:
            db.commit()
        else:
            click.echo("Dryrun")
            db.rollback()


@ingest.group()
def lightcurves():
    pass


@lightcurves.command()
@click.argument("lightcurve_directories", type=pathlib.Path, nargs=-1)
@click.option("--tic-catalog/--tic-db", is_flag=True, default=True)
@click.option(
    "--tic-catalog-path-template",
    type=str,
    default=jobs.DirectoryPlan.DEFAULT_TIC_CATALOG_TEMPLATE,
    show_default=True,
)
@click.option(
    "--quality-flag-template",
    type=str,
    default=jobs.DirectoryPlan.DEFAULT_QUALITY_FLAG_TEMPLATE,
    show_default=True,
)
@click.option("--scratch", type=click.Path(file_okay=False, exists=True))
def ingest_dir(
    ctx,
    paths,
    tic_catalog,
    tic_catalog_path_template,
    quality_flag_template,
    scratch,
):
    with tempfile.TemporaryDirectory(dir=scratch) as tempdir:
        tempdir_path = pathlib.Path(tempdir)
        cache_path = tempdir_path / "db.sqlite3"
        contexts.make_shared_context(cache_path)
        with db_from_config(ctx.obj["dbconf"]) as db:
            contexts.populate_ephemeris(cache_path, db)
            contexts.populate_tjd_mapping(cache_path, db)

            for directory in paths:
                logger.info(f"Considering {directory}")

            plan = jobs.DirectoryPlan(
                paths, ctx.obj["dbconf"], recursive=False
            )

            h5_jobs = plan.get_jobs()
            if tic_catalog:
                path_iter = plan.yield_needed_tic_catalogs(
                    path_template=tic_catalog_path_template
                )
                for catalog_path in path_iter:
                    contexts.populate_tic_catalog(cache_path, catalog_path)
            else:
                tic_ids = plan.tic_ids
                contexts.populate_tic_catalog_w_db(cache_path, tic_ids)

            for args in plan.yield_needed_quality_flags(
                path_template=quality_flag_template
            ):
                logger.debug(f"Requiring quality flags {args}")
                contexts.populate_quality_flags(cache_path, *args)

        lightcurve_arrays.ingest_jobs(
            ctx.obj,
            h5_jobs,
            cache_path,
        )
        logger.success("Done!")


@lightcurves.command()
@click.pass_context
@click.argument("tic_file", type=click.Path(dir_okay=False, exists=True))
@click.option(
    "--quality-flag-template",
    type=str,
    default=jobs.DirectoryPlan.DEFAULT_QUALITY_FLAG_TEMPLATE,
)
@click.option("--scratch", type=click.Path(file_okay=False, exists=True))
def ingest_tic_list(
    ctx,
    tic_file,
    quality_flag_template,
    scratch,
):
    tic_ids = set(map(int, open(tic_file, "rt").readlines()))
    with tempfile.TemporaryDirectory(dir=scratch) as tempdir:
        cache_path = pathlib.Path(tempdir, "db.sqlite3")
        contexts.make_shared_context(cache_path)
        with db_from_config(ctx.obj["dbconf"]) as db:
            contexts.populate_ephemeris(cache_path, db)
            contexts.populate_tjd_mapping(cache_path, db)

        plan = jobs.TICListPlan(tic_ids, ctx.obj["dbconf"])
        h5_jobs = plan.get_jobs()

        contexts.populate_tic_catalog_w_db(cache_path, tic_ids)

        for args in plan.yield_needed_quality_flags(
            path_template=quality_flag_template
        ):
            logger.debug(f"Requiring quality flags {args}")
            contexts.populate_quality_flags(cache_path, *args)

        lightcurve_arrays.ingest_jobs(
            ctx.obj,
            h5_jobs,
            cache_path,
        )
        click.echo("Done!")


EPH_COLS = {
    "JDTDB": "barcentric_dynamical_time",
    "Calendar Date (TDB)": "calendar_date",
    "X": "x_coordinate",
    "Y": "y_coordinate",
    "Z": "z_coordinate",
    "LT": "light_travel_time",
    "RG": "range_to",
    "RR": "range_rate",
}


@ingest.command()
@click.pass_context
@click.argument(
    "ephemeris_csv", nargs=-1, type=click.Path(dir_okay=False, exists=True)
)
def spacecraft(ctx, ephemeris_csv):
    ephemeris_dfs = [
        pd.read_csv(
            f,
            comment="#",
            header=0,
            names=[
                "barycentic_dynamical_time",
                "calendar_date",
                "x_coordinate",
                "y_coordinate",
                "z_coordinate",
                "light_travel_time",
                "range_to",
                "range_rate",
            ],
            usecols=[0, 1, 2, 3, 4, 5, 6, 7],
            index_col=0,
        )
        for f in ephemeris_csv
    ]

    ephemeris_df = pd.concat(ephemeris_dfs, sort=True)
    # Remove prefix 'A.D '
    ephemeris_df["calendar_date"] = ephemeris_df["calendar_date"].apply(
        lambda r: r.replace("A.D. ", "")
    )

    ephemeris_df = ephemeris_df[~ephemeris_df.index.duplicated(keep="last")]
    ephemeris_df.sort_index(inplace=True)

    with db_from_config(ctx.obj["dbconf"]) as db:
        for i, row in ephemeris_df.iterrows():
            eph = m.SpacecraftEphemeris(
                barycentric_dynamical_time=i, **dict(row)
            )
            db.merge(eph)
            click.echo("Added {0}".format(eph))
        if not ctx.obj["dryrun"]:
            db.commit()
            click.echo(click.style("Committed changes!", fg="green"))
