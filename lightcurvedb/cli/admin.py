import click
import os
from glob import glob
from functools import partial
from sqlalchemy import text, func, and_
from tabulate import tabulate
from multiprocessing import Pool

from lightcurvedb import db
from lightcurvedb.models import (
    Observation,
    Orbit,
    Frame,
    Lightpoint,
    Lightcurve,
)
from lightcurvedb.cli.types import CommaList
from lightcurvedb.io.feeder import yield_lightcurve_data
from lightcurvedb.util.iter import chunkify

from . import lcdbcli


def get_mask(scratch_dir="/scratch/tmp/lcdb_ingestion"):
    recovered_files = glob(os.path.join(scratch_dir, "*.ls"))
    ids = set()
    for catalog in recovered_files:
        for line in open(catalog, "rt").readlines():
            try:
                ids.add(int(line))
            except (ValueError, TypeError):
                continue
    return ids


def recover(maximum_missing, lightcurve_ids):
    pid = os.getpid()
    to_ingest = []
    fine = []
    with db:
        cadence_map = dict(
            db.query(Frame.cadence, Orbit.orbit_number)
            .join(Frame.orbit)
            .filter(Frame.frame_type_id == "Raw FFI")
        )
        for lightcurve_id in lightcurve_ids:
            existing_cadences = db.query(Lightpoint.cadence).filter_by(
                lightcurve_id=lightcurve_id
            )
            missing_cadences = (
                db.query(Frame.cadence)
                .join(
                    Observation,
                    and_(
                        Observation.orbit_id == Frame.orbit_id,
                        Observation.camera == Frame.camera,
                    ),
                )
                .filter(
                    Observation.lightcurve_id == lightcurve_id,
                    Frame.frame_type_id == "Raw FFI",
                    ~Frame.cadence.in_(existing_cadences.subquery()),
                )
                .all()
            )

            if len(missing_cadences) > maximum_missing:
                orbits_missing = sorted(
                    set(cadence_map[cadence] for cadence, in missing_cadences)
                )
                click.echo(
                    "Lightcurve {0} missing orbits {1}".format(
                        lightcurve_id, ", ".join(map(str, orbits_missing))
                    )
                )
                orbit_ids = [
                    n
                    for n, in db.query(Orbit.id).filter(
                        Orbit.orbit_number.in_(orbits_missing)
                    )
                ]
                db.query(Observation).filter(
                    Observation.lightcurve_id == lightcurve_id,
                    Observation.orbit_id.in_(orbit_ids),
                ).delete(synchronize_session=False)
                db.commit()
                to_ingest.append(lightcurve_id)
            else:
                click.echo("Lightcurve {0} is fine".format(lightcurve_id))
                fine.append(lightcurve_id)
    with open(
        "/scratch/tmp/lcdb_ingestion/{0}_to_reingest.ls".format(pid), "at"
    ) as fout:
        fout.write("\n".join(map(str, to_ingest)))
        if len(to_ingest) > 0:
            fout.write("\n")
    with open(
        "/scratch/tmp/lcdb_ingestion/{0}_fine.ls".format(pid), "at"
    ) as fout:
        fout.write("\n".join(map(str, fine)))
        if len(fine) > 0:
            fout.write("\n")


@lcdbcli.group()
@click.pass_context
def administration(ctx):
    """Base LCDB Administration Commands"""
    click.echo("Entering administration context, please use responsibly!" "")


# Define procedure cli commands
@administration.group()
@click.pass_context
def procedures(ctx):
    pass


@procedures.command()
@click.pass_context
def reload(ctx):
    from lightcurvedb.io.procedures.procedure import _yield_procedure_ddl

    with ctx.obj["dbconf"] as db:
        for ddl in _yield_procedure_ddl():
            click.echo("Executing {0}".format(ddl))
            db.execute(ddl)
        if not ctx.obj["dryrun"]:
            click.echo("Committing...")
            db.commit()
            click.echo("Success")
        else:
            click.echo("Rolling back!")
            db.rollback()


@procedures.command()
@click.pass_context
def list_defined(ctx):
    """
    List defined PostgreSQL Stored Procedures
    """
    # TODO, define statement using SQLAlchemy constructs
    # Currently, shamelessly copied from
    # https://stackoverflow.com/questions/1347282/how-can-i-get-a-list-of-all-functions-stored-in-the-database-of-a-particular-sch
    RAW_SQL = """
        SELECT
            routines.routine_name,
            parameters.data_type,
            parameters.ordinal_position
        FROM information_schema.routines
            LEFT JOIN information_schema.parameters
            ON routines.specific_name=parameters.specific_name
            WHERE routines.specific_schema='my_specified_schema_name'
            ORDER BY routines.routine_name, parameters.ordinal_position;
    """
    with ctx.obj["dbconf"] as db:
        results = db.execute(text(RAW_SQL))
        click.echo(
            tabulate(
                results,
                headers=["Routine Name", "Data Type", "Ordinal Position"],
            )
        )


@administration.group()
@click.pass_context
def maintenance(ctx):
    pass


@maintenance.command()
@click.pass_context
@click.argument("orbits", type=int, nargs=-1)
@click.option("--cameras", type=CommaList(int), default="1,2,3,4")
@click.option("--ccds", type=CommaList(int), default="1,2,3,4")
@click.option("--n-searchers", type=int, default=10)
@click.option("--maximum-missing", type=click.IntRange(min=10), default=10)
def delete_invalid_orbit_observations(
    ctx, orbits, cameras, ccds, n_searchers, maximum_missing
):
    with ctx.obj["dbconf"] as db:
        q = (
            db.query(Observation.lightcurve_id)
            .join(Observation.orbit)
            .filter(
                Orbit.orbit_number.in_(orbits),
                Observation.camera.in_(cameras),
                Observation.ccd.in_(ccds),
            )
            .distinct()
        )
        click.echo("Querying ids")
        ids = {id_ for id_, in q}
        click.echo("Building cadence to orbit number map")

    mask = get_mask()
    ids = ids - mask

    click.echo("Gonna process {0} lightcurves".format(len(ids)))
    jobs = chunkify(ids, 1000)
    with Pool(n_searchers) as pool:
        func = partial(recover, maximum_missing)
        pool.map(func, jobs)


@maintenance.command()
@click.pass_context
@click.argument("tics", type=int, nargs=-1)
@click.option("--maximum-missing", type=click.IntRange(min=10), default=10)
def delete_invalid_tic_observations(ctx, tics, maximum_missing):
    with ctx.obj["dbconf"] as db:
        q = (
            db.query(Observation.lightcurve_id)
            .join(Observation.lightcurve)
            .filter(Lightcurve.tic_id.in_(tics))
            .distinct()
        )
        click.echo("Querying ids")
        ids = [id_ for id_, in q]
        click.echo("Building cadence to orbit number map")
        cadence_map = dict(
            db.query(Frame.cadence, Orbit.orbit_number)
            .join(Frame.orbit)
            .filter(Frame.frame_type_id == "Raw FFI")
        )

        click.echo("Gonna process {0} lightcurves".format(len(ids)))
        for lightcurve_id in ids:
            existing_cadences = db.query(Lightpoint.cadence).filter_by(
                lightcurve_id=lightcurve_id
            )
            missing_cadences = (
                db.query(Frame.cadence)
                .join(
                    Observation,
                    and_(
                        Observation.orbit_id == Frame.orbit_id,
                        Observation.camera == Frame.camera,
                    ),
                )
                .filter(
                    Observation.lightcurve_id == lightcurve_id,
                    Frame.frame_type_id == "Raw FFI",
                    ~Frame.cadence.in_(existing_cadences.subquery()),
                )
                .all()
            )

            if len(missing_cadences) > maximum_missing:
                orbits_missing = sorted(
                    set(cadence_map[cadence] for cadence, in missing_cadences)
                )
                click.echo(
                    "Lightcurve {0} missing orbits {1}".format(
                        lightcurve_id, ", ".join(map(str, orbits_missing))
                    )
                )
                orbit_ids = [
                    n
                    for n, in db.query(Orbit.id).filter(
                        Orbit.orbit_number.in_(orbits_missing)
                    )
                ]
                db.query(Observation).filter(
                    Observation.lightcurve_id == lightcurve_id,
                    Observation.orbit_id.in_(orbit_ids),
                ).delete(synchronize_session=False)
                db.commit()

            else:
                click.echo("Lightcurve {0} is fine".format(lightcurve_id))
