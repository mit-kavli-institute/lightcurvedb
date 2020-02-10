import click
from lightcurvedb.models import Aperture
from . import lcdbcli

@lcdbcli.command()
@click.pass_context
@click.argument('name', type=str)
@click.argument('aperture-string', type=str)
def aperture(ctx, name, aperture_string):
    with ctx.obj['dbconf'] as db:
        check = db.session.query(Aperture).filter(Aperture.name == name).one_or_none()
        s_r, i_r, o_r = Aperture.from_aperture_string(aperture_string)
        if check:
            # Update
            click.echo(click.style('Updating {}'.format(check), fg='yellow'))
            value = click.prompt(
                'Enter a new name (empty input is considered to be no change)',
                type=str,
                default=check.name
            )
            if value:
                check.name = value
        else:
            # Inserting
            aperture = Aperture(name=name, star_radius=s_r, inner_radius=i_r, outer_radius=o_r)
            click.echo(click.style('Creating aperture {}'.format(aperture), fg='green'))

        if not ctx.obj['dryrun']:
            if check:
                click.echo(click.style('Will update {}'.format(check), fg='yellow'))
            else:
                click.echo(click.style('Will insert {}'.format(aperture), fg='green'))
                db.add(aperture)
            prompt = click.style('Do these changes look ok?', bold=True)
            click.confirm(prompt, abort=True)
            db.commit()

