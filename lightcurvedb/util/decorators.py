"""
This module defines utility decorators.
"""

from functools import wraps
from warnings import catch_warnings, simplefilter


def cast_to(type_):
    def external_wrap(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            return type_(func)
        return wrapper
    return external_wrap


def suppress_warnings(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        with catch_warnings():
            simplefilter('ignore')
            return func(*args, **kwargs)

    return wrapper
