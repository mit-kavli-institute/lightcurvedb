import click
import pandas as pd
import sqlalchemy as sa
from loguru import logger

from lightcurvedb import db_from_config
from lightcurvedb import models as m
from lightcurvedb.util.tess import gps_time_to_datetime

from .base import lcdbcli

COLS = {
    "JDTDB": "barcentric_dynamical_time",
    "Calendar Date (TDB)": "calendar_date",
    "X": "x_coordinate",
    "Y": "y_coordinate",
    "Z": "z_coordinate",
    "LT": "light_travel_time",
    "RG": "range_to",
    "RR": "range_rate",
}


@lcdbcli.group()
@click.pass_context
def spacecraft(ctx):
    """
    Commands for ingesting spacecraft data.
    """
    pass


@spacecraft.command()
@click.pass_context
@click.argument(
    "ephemeris_csv", nargs=-1, type=click.Path(dir_okay=False, exists=True)
)
@click.option("--update-on-date-collision", is_flag=True)
@click.option("--ignore-on-date-collision", is_flag=True)
@click.option("--only-consider-past-sector", type=int, default=None)
def ingest(
    ctx,
    ephemeris_csv,
    update_on_date_collision,
    ignore_on_date_collision,
    only_consider_past_sector,
):

    if update_on_date_collision and ignore_on_date_collision:
        raise click.Abort("Update and ignore flags cannot both be set!")

    if only_consider_past_sector is not None:
        with db_from_config(ctx.obj["dbconf"]) as db:
            q = (
                sa.select(sa.func.max(m.Frame.gps_time))
                .join(m.Frame.orbit)
                .where(m.Orbit.sector == only_consider_past_sector)
            )

        gps_cutoff = db.execute(q).fetchone()[0]
        date_cutoff = gps_time_to_datetime(gps_cutoff)

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
    ephemeris_df["calendar_date"] = pd.to_datetime(
        ephemeris_df["calendar_date"].apply(lambda r: r.replace("A.D. ", ""))
    )

    ephemeris_df = ephemeris_df[~ephemeris_df.index.duplicated(keep="last")]
    ephemeris_df.sort_index(inplace=True)

    if only_consider_past_sector is not None:
        cur_len = len(ephemeris_df)
        ephemeris_df = ephemeris_df[
            ephemeris_df["calendar_date"] > date_cutoff
        ]
        cut_len = len(ephemeris_df)
        logger.info(
            f"Reduced position payload from {cur_len} to {cut_len} "
            f"after sector date cutoff {date_cutoff}"
        )

    min_bjd = min(ephemeris_df.index)
    max_bjd = max(ephemeris_df.index)

    mask_q = sa.select(m.SpacecraftEphemeris.bjd).where(
        m.SpacecraftEphemeris.bjd.between(min_bjd, max_bjd)
    )

    with db_from_config(ctx.obj["dbconf"]) as db:

        bjd_mask = set(db.execute(mask_q).scalars().all())
        for i, row in ephemeris_df.iterrows():
            if i in bjd_mask:
                if ignore_on_date_collision:
                    logger.info(f"Ignoring colliding date {i}")
                if update_on_date_collision:
                    logger.info(f"Updating colliding date {i}...")
                    q = (
                        sa.update(m.SpacecraftEphemeris)
                        .where(m.SpacecraftEphemeris.bjd == i)
                        .values(**dict(row))
                    )
                    db.execute(q)
            else:
                eph = m.SpacecraftEphemeris(
                    barycentric_dynamical_time=i, **dict(row)
                )
                db.add(eph)
                logger.info(f"Added {eph}")
        if not ctx.obj["dryrun"]:
            db.commit()
            logger.success("Committed changes!")
        else:
            logger.warning("Respecting dryrun, rolling back changes")
