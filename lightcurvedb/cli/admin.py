import os
from functools import partial
from glob import glob
from multiprocessing import Pool

import click
from sqlalchemy import and_, func, text
from tabulate import tabulate
from tqdm import tqdm

from lightcurvedb import db
from lightcurvedb.cli.types import CommaList, ModelField
from lightcurvedb.cli.utils import tabulate_query
from lightcurvedb.core import admin as psql_admin
from lightcurvedb.core.psql_tables import PGStatActivity
from lightcurvedb.models import (
    Frame,
    Lightcurve,
    Lightpoint,
    Observation,
    Orbit,
)
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
        orbit_map = dict(db.query(Orbit.id, Orbit.orbit_number))
        for id_ in sorted(lightcurve_ids):
            existing_cadence_q = (
                db.query(Lightpoint.cadence).filter_by(lightcurve_id=id_)
            ).subquery("existing_cadences")

            missing_orbits = (
                db.query(Frame.orbit_id, func.count(Frame.orbit_id))
                .join(
                    Observation,
                    and_(
                        Observation.orbit_id == Frame.orbit_id,
                        Observation.camera == Frame.camera,
                    ),
                )
                .filter(
                    ~Frame.cadence.in_(existing_cadence_q),
                    Frame.frame_type_id == "Raw FFI",
                    Observation.lightcurve_id == id_,
                )
                .group_by(Frame.orbit_id)
            )
            missing_orbits = [
                orbit_id
                for orbit_id, count in missing_orbits
                if count >= maximum_missing
            ]
            if missing_orbits:
                db.query(Observation).filter(
                    Observation.orbit_id.in_(missing_orbits),
                    Observation.lightcurve_id == id_,
                ).delete(synchronize_session=False)
                db.commit()
                with open(
                    "/scratch/tmp/lcdb_ingestion/{0}_to_reingest.ls".format(
                        pid
                    ),
                    "at",
                ) as fout:
                    fout.write("{0}\n".format(id_))
                to_ingest.append(id_)
            else:
                with open(
                    "/scratch/tmp/lcdb_ingestion/{0}_fine.ls".format(pid), "at"
                ) as fout:
                    fout.write("{0}\n".format(id_))
                fine.append(id_)

    return fine, to_ingest


@lcdbcli.group()
@click.pass_context
def admin(ctx):
    """Base LCDB admin Commands"""
    click.echo("Entering admin context, please use responsibly!" "")


# Define procedure cli commands
@admin.group()
@click.pass_context
def procedures(ctx):
    """
    Base SQL Procedure Commands
    """


@procedures.command()
@click.pass_context
def reload(ctx):
    """
    Read the defined SQL files and submit any changes to the database.
    """
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


@admin.group()
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
    mask = get_mask()
    ids = []
    skipped = 0

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
        for (id_,) in q:
            if id_ not in mask:
                ids.append(id_)
            else:
                skipped += 1
    click.echo(
        "Ignoring {0} ids as they have already been processed".format(skipped)
    )

    click.echo("Gonna process {0} lightcurves".format(len(ids)))
    jobs = list(chunkify(ids, 1000))
    with Pool(n_searchers) as pool:
        func = partial(recover, maximum_missing)
        results = pool.imap_unordered(func, jobs)
        with tqdm(results, total=len(jobs)) as bar:
            for result in bar:
                bar.write(
                    "{0} OK lightcurves, {1} bad lightcurves".format(
                        len(result[0]), len(result[1])
                    )
                )
            bar.update(1)


@maintenance.command()
@click.pass_context
@click.argument("tics", type=int, nargs=-1)
@click.option("--maximum-missing", type=click.IntRange(min=10), default=10)
def delete_invalid_tic_observations(ctx, tics, maximum_missing):
    with ctx.obj["dbconf"] as db:
        q = db.query(Lightcurve.id).filter(Lightcurve.tic_id.in_(tics))
        click.echo("Querying ids")
        ids = sorted(id_ for id_, in q)
    fine, to_reingest = recover(maximum_missing, ids)
    click.echo("OK IDS: {0}".format(fine))
    click.echo("BAD IDS: {0}".format(to_reingest))


@admin.group()
@click.pass_context
def state(ctx):
    pass


@state.command()
@click.pass_context
@click.option(
    "--column",
    "-c",
    "columns",
    multiple=True,
    type=ModelField(PGStatActivity),
    default=["pid", "state", "application_name", "query"],
)
def get_all_queries(ctx, columns):
    with ctx.obj["dbconf"] as db:
        q = db.query(*columns).filter(
            PGStatActivity.database == "lightpointdb"
        )
        click.echo(tabulate_query(q))


@state.command()
@click.pass_context
@click.option(
    "--column",
    "-c",
    "columns",
    multiple=True,
    type=ModelField(PGStatActivity),
    default=["pid", "query", "application_name", "blocked_by"],
)
def get_blocked_queries(ctx, columns):
    with ctx.obj["dbconf"] as db:
        q = db.query(*columns).filter(
            PGStatActivity.database == "lightpointdb",
            PGStatActivity.is_blocked(),
        )
        click.echo(tabulate_query(q))


@state.command()
@click.pass_context
@click.argument("pids", type=int, nargs=-1)
@click.option(
    "--column",
    "-c",
    "columns",
    multiple=True,
    type=ModelField(PGStatActivity),
    default=["pid", "state", "application_name", "query"],
)
def get_info(ctx, pids, columns):
    with ctx.obj["dbconf"] as db:
        q = db.query(*columns).filter(PGStatActivity.pid.in_(pids))
        click.echo(tabulate_query(q))


@state.command()
@click.pass_context
@click.argument("pids", type=int, nargs=-1)
def terminate(ctx, pids):
    with ctx.obj["dbconf"] as db:
        queries = db.query(PGStatActivity.query).filter(
            PGStatActivity.pid.in_(pids)
        )

        if queries.count() == 0:
            click.echo("No queries with pids {pids} exist")
            return 0

        click.echo(click.style("Will terminate..."))
        for (query,) in queries:
            click.echo(f"\t{query}")
        prompt_msg = click.style(
            "TERMINATE THESE QUERIES?", bg="red", blink=True
        )

        click.confirm(prompt_msg, abort=True, default=False)

        db.query(PGStatActivity.terminate).filter(
            PGStatActivity.pid.in_(pids)
        ).all()
        click.echo("Terminated")
