from lightcurvedb import models


def qlp_type_check(db, Model, check):
    if isinstance(check, Model):
        return Model.id == check.id
    x = db.session.query(Model).filter(Mode.name == check).one()
    return Model.id == check.id


def qlp_type_multiple_check(db, Model, checks):
    check_filter = []

    if isinstance(checks[0], Model):
        # Assume mode is that everything is a Model instance
        check_filter = [c.id for c in checks]
    else:
        # Assume mode is that everything is a string
        check_filter = db.session.query(Model.id).filter(Model.name.in_(checks)).all()

    return check_filter
