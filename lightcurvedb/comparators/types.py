from lightcurvedb import models


def qlp_type_check(Model, check):
    if isinstance(check, str):
        return Model.name == check
    return Model.id == check.id
