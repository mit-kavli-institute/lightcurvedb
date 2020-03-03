from lightcurvedb import models


def qlp_type_check(Model, check):
    if isinstance(check, Model):
        return Model.id == check.id
    return Model.name == check


def qlp_type_multiple_check(Model, checks):
    check_filter = []
    for check in checks:
        if isinstance(check, Model):
            check_filter.append(check.name)
        else:
            check_filter.append(check)
    return Model.name.in_(check_filter)
