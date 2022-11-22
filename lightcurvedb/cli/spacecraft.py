import click
import pandas as pd

from lightcurvedb.models import SpacecraftEphemeris

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
def ingest(ctx, ephemeris_csv):
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

    with ctx.obj["dbfactory"]() as db:
        for i, row in ephemeris_df.iterrows():
            eph = SpacecraftEphemeris(
                barycentric_dynamical_time=i, **dict(row)
            )
            db.session.merge(eph)
            click.echo("Added {0}".format(eph))
        if not ctx.obj["dryrun"]:
            db.commit()
            click.echo(click.style("Committed changes!", fg="green"))
