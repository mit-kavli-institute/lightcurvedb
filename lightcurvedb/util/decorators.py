"""
This module defines utility decorators.
"""

from functools import wraps
from warnings import catch_warnings, simplefilter
from time import time


def cast_to(type_):
    def external_wrap(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            return type_(result)

        return wrapper

    return external_wrap


def suppress_warnings(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        with catch_warnings():
            simplefilter("ignore")
            return func(*args, **kwargs)

    return wrapper


def track_runtime(func):
    """
    Decorate the function to return time elapsed information
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        t0 = time()
        result = func(*args, **kwargs)
        elapsed = time() - t0
        return result, elapsed
    return wrapper
