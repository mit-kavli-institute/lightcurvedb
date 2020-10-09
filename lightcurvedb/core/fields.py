from sqlalchemy import Column
from sqlalchemy.dialects import postgresql as psql


def high_precision_column(precision=None, asdecimal=False, **column_args):
    return Column(
        psql.DOUBLE_PRECISION(precision=precision, asdecimal=asdecimal),
        **column_args
    )
