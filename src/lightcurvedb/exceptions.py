"""
Base module for all lightcurvedb specific exceptions.
"""
from sqlalchemy.dialects import postgresql


class LightcurveDBException(Exception):
    """Base exception for all manually thrown exceptions that do not
    fall within the core Python Exception classes.
    """

    pass


class PrimaryIdentNotFound(LightcurveDBException):
    """
    Raised when attempting to resolve a single model instance but the
    given identity does not match to any record
    """

    pass


class EmptyLightcurve(LightcurveDBException):
    """
    Raised when a given lightcurve has a definition but no timeseries data.
    """

    def __init__(self, q):
        statement = q.compile(dialect=postgresql.dialect())
        super().__init__(
            "Could not find any lightcurves with "
            f"the given context: {statement}"
        )
