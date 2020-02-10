import click
from lightcurvedb.models import LightcurveType
from . import lcdbcli

@lcdbcli.command()
@click.pass_context
@click.argument('lightcurve-type-name', type=str)
def create_lightcurvetype(ctx, lightcurve_type_name):
    with ctx.obj['dbconf'] as db:
        check = db.session.query(LightcurveType).filter(LightcurveType == lightcurve_type_name).one_or_none()
        if check:
            # Updating
            click.echo(click.style('Updating {}'.format(check), fg='yellow'))
            value = click.prompt(
                'Enter a new name (empty input is considered to be no change)',
                type=str,
                default=check.name)
            if value:
                check.name = value
            value = click.prompt(
                'Enter a description (empty input is considered to be no change)',
                type=str,
                default=check.description)
        else:
            # Inserting
            click.echo(click.style('Creating new frame type {}'.format(lightcurve_type_name), fg='green'))
            desc = click.prompt('Enter a description for {}'.format(lightcurve_type_name))
            new_type = LightcurveType(name=lightcurve_type_name, description=desc)

        if not ctx.obj['dryrun']:
            if check:
                click.echo(click.style('Update on: {}'.format(check), fg='yellow'))
            else:
                click.echo(click.style('Inserting {}'.format(new_type), fg='green'))
                db.add(new_type)
            prompt = click.style('Do these changes look ok?', bold=True)
            click.confirm(prompt, abort=True)
            db.commit()

