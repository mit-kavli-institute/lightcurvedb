import click
from lightcurvedb.models import LightcurveType
from lightcurvedb.core.base_model import QLPDataSubType
from . import lcdbcli


def get_default(column, db):
    default = column.default
    resolve = False
    if not default:
        default = column.server_default
        resolve = True
    if not default:
        return None, False  # No default

    if resolve:
        return db.session.execute(default.arg).fetchone()[0], True
    return default, False



@lcdbcli.group()
@click.pass_context
def data_types(ctx):
    pass


@data_types.command()
@click.pass_context
@click.argument("model", type=str)
def add_type(ctx, model):
    DataModel = QLPDataSubType.get_model(model)

    kwargs = {}

    with ctx.obj["dbconf"] as db:
        for column in DataModel.__table__.columns:
            default_val, was_resolved = get_default(column, db)

            if was_resolved:
                click.echo(
                    click.style(
                        "Column {0} uses server-side defaults which are determined "
                        "upon insertion. This default may not reflect actual "
                        "value.".format(column.name)
                    )
                )
            prompt_kwargs = {
                'type': column.type.python_type
            }

            if default_val:
                prompt_kwargs['default'] = default_val

            value = click.prompt(
                "Enter value for {0}".format(
                    column.name
                ),
                **prompt_kwargs
            )

            if value is not None:
                kwargs[column.name] = value

        instance = DataModel(**kwargs)

        db.add(instance)
        if ctx.obj["dryrun"]:
            db.rollback()
        else:
            db.commit()


@data_types.command()
@click.pass_context
@click.argument("lightcurve-type-name", type=str)
def add_lightcurvetype(ctx, lightcurve_type_name):
    """
    Add a lightcurve type to the database.
    """
    with ctx.obj["dbconf"] as db:
        check = (
            db.session.query(LightcurveType)
            .filter(LightcurveType == lightcurve_type_name)
            .one_or_none()
        )
        if check:
            # Updating
            click.echo(click.style("Updating {0}".format(check), fg="yellow"))
            value = click.prompt(
                "Enter a new name "
                "(empty input is considered to be no change)",
                type=str,
                default=check.name,
            )
            if value:
                check.name = value
            value = click.prompt(
                "Enter a description "
                "(empty input is considered to be no change)",
                type=str,
                default=check.description,
            )
        else:
            # Inserting
            click.echo(
                click.style(
                    "Creating new frame type {0}".format(lightcurve_type_name),
                    fg="green",
                )
            )
            desc = click.prompt(
                "Enter a description for {0}".format(lightcurve_type_name)
            )
            new_type = LightcurveType(
                name=lightcurve_type_name, description=desc
            )

        if not ctx.obj["dryrun"]:
            if check:
                click.echo(
                    click.style("Update on: {0}".format(check), fg="yellow")
                )
            else:
                click.echo(
                    click.style("Inserting {0}".format(new_type), fg="green")
                )
                db.add(new_type)
            prompt = click.style("Do these changes look ok?", bold=True)
            click.confirm(prompt, abort=True)
            db.commit()


@lcdbcli.command()
@click.pass_context
@click.argument("lightcurve-type-name", type=str)
def delete_lightcurvetype(ctx, lightcurve_type_name):
    """
    Add a lightcurve type to the database.
    """
    with ctx.obj["dbconf"] as db:
        check = (
            db.session.query(LightcurveType)
            .filter(LightcurveType == lightcurve_type_name)
            .one_or_none()
        )
        if check:
            # Updating
            click.echo(click.style("Removing {0}".format(check), fg="yellow"))
            db.session.delete()
        else:
            # No lightcurve type exists
            pass

        if not ctx.obj["dryrun"]:
            if check:
                click.echo(
                    click.style("Update on: {0}".format(check), fg="yellow")
                )
            else:
                click.echo(
                    click.style("Inserting {0}".format(new_type), fg="green")
                )
                db.add(new_type)
            prompt = click.style("Do these changes look ok?", bold=True)
            click.confirm(prompt, abort=True)
            db.commit()
