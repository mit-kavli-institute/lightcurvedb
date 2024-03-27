from sqlalchemy import types

_SQL_ALIASES = {
    "bigint": types.BigInteger,
    "biginteger": types.BigInteger,
    "bool": types.Boolean,
    "boolean": types.Boolean,
    "date": types.Date,
    "datetime": types.DateTime,
    "float": types.Float,
    "int": types.Integer,
    "integer": types.Integer,
    "interval": types.Interval,
    "largebinary": types.LargeBinary,
    "numeric": types.Numeric,
    "pickle": types.PickleType,
    "pickletype": types.PickleType,
    "smallint": types.SmallInteger,
    "smallinteger": types.SmallInteger,
    "text": types.Text,
    "time": types.Time,
}


def _str_to_sql_type(string):
    try:
        return _SQL_ALIASES[string.lower()]
    except KeyError:
        raise KeyError(f"Unknown specified type {string}")


def _resolve_type(type_):
    if isinstance(type_, str):
        return _str_to_sql_type(type_)
    return type_


def psql_safe_str(string):
    """
    Replace NULL char in a string with \uFFFD. A copy of a string will
    be returned.

    Parameters
    ----------
    string: str
        The string to replace

    Returns
    -------
    str
        The psql safe string.
    """
    if isinstance(string, str):
        return string.replace("\x00", "\uFFFD")
    return string
