from sqlalchemy import Column
from sqlalchemy.dialects import postgresql as psql
from sqlalchemy.schema import CreateColumn
from sqlalchemy.ext.compiler import compiles
from psycopg2.extensions import register_adapter, AsIs, Float
from numpy import NaN


def high_precision_column(precision=None, asdecimal=False, **column_args):
    return Column(
        psql.DOUBLE_PRECISION(precision=precision, asdecimal=asdecimal),
        **column_args
    )


@compiles(CreateColumn, "postgresql")
def use_identity(element, compiler, **kw):
    text = compiler.visit_create_column(element, **kw)
    text = text.replace("SERIAL", "INT GENERATED BY DEFAULT AS IDENTITY")
    return text


def nan_safe_adapter(f):
    """
    Convert numpy NaN objects to "nan". SQLAlchemy emits np.NaN as unquoted
    nan strings, resulting in postgres looking for a column literally named
    "nan". Convert these to strings so postgres can safely cast the value.
    """
    if f is NaN:
        return Float(float('nan'))
    return Float(f)


register_adapter(float, nan_safe_adapter)
