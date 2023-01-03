from math import isnan


def isiterable(x):
    try:
        iter(x)
        return True
    except TypeError:
        return False


def safe_float(x):
    try:
        return float(x)
    except (ValueError, TypeError):
        return float("nan")


def sql_nan_cast(x):
    return "NaN" if isnan(x) else x
