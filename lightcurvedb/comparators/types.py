from lightcurvedb import models


def qlp_type_check(db, Model, check):
    if isinstance(check, Model):
        return Model.id == check.id
    x = db.session.query(Model).filter(Mode.name == check).one()
    return Model.id == check.id


def qlp_type_multiple_check(Model, checks):
    check_filter = []
    for check in checks:
        if isinstance(check, Model):
            check_filter.append(check.name)
        else:
            check_filter.append(check)
    return Model.name.in_(check_filter)
