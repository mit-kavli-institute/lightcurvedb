def isiterable(x):
    try:
        iter(x)
        return True
    except TypeError:
        return False


def safe_float(x):
    try:
        return float(x)
    except ValueError:
        return float("nan")
