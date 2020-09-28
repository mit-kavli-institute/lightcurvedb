"""
This module defines utility decorators.
"""

from functools import wraps
from warnings import catch_warnings, simplefilter


def suppress_warnings(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        with catch_warnings():
            simplefilter('ignore')
            return func(*args, **kwargs)

    return wrapper
