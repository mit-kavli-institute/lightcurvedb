from lightcurvedb import models


def qlp_type_check(Model, check):
    if isinstance(check, str):
        return Model.name == check
    return Model.id == check.id


def qlp_type_multiple_check(Model, checks):
    check_filter = []
    for check in checks:
        if isinstance(check, str):
            check_filter.append(check)
        else:
            check_filter.append(check.name)
    return Model.name.in_(check_filter)
