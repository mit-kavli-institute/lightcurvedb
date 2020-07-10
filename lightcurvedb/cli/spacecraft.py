import click
import pandas as pd
from lightcurvedb.models import SpacecraftEphemris
from .base import lcdbcli

COLS = {
    'JDTDB': 'barcentric_dynamical_time',
    'Calendar Date (TDB)': 'calendar_date',
    'X': 'x_coordinate',
    'Y': 'y_coordinate',
    'Z': 'z_coordinate',
    'LT': 'light_travel_time',
    'RG': 'range_to',
    'RR': 'range_rate'
}

@lcdbcli.group()
@click.pass_context
def spacecraft(ctx):
    pass

@spacecraft.command()
@click.pass_context
@click.argument('ephemris_csv', nargs=-1, type=click.Path(dir_okay=False, exists=True))
def ingest(ctx, ephemris_csv):
    ephemris_dfs = [
        pd.read_csv(
            f,
            comment='#',
            header=0,
            names=[
                'barycentic_dynamical_time',
                'calendar_date',
                'x_coordinate',
                'y_coordinate',
                'z_coordinate',
                'light_travel_time',
                'range_to',
                'range_rate'
            ],
            usecols=[0,1,2,3,4,5,6,7],
            index_col=0)
        for f in ephemris_csv
    ]

    ephemris_df = pd.concat(ephemris_dfs, sort=True)
    # Remove prefix 'A.D '
    ephemris_df['calendar_date'] = ephemris_df['calendar_date'].apply(
        lambda r: r.replace('A.D. ', '')
    )


    ephemris_df = ephemris_df[~ephemris_df.index.duplicated(keep='last')]
    ephemris_df.sort_index(inplace=True)

    with ctx.obj['dbconf'] as db:
        for i, row in ephemris_df.iterrows():
            eph = SpacecraftEphemris(
                barycentric_dynamical_time=i,
                **dict(row)
            )
            db.session.merge(eph)
            click.echo(
                f'Added {eph}'
            )
        if not ctx.obj['dryrun']:
            db.commit()
            click.echo(
                click.style(
                    'Committed changes!',
                    fg='green'
                )
            )
