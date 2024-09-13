from pathlib import Path
from typing import Any

from psycopg import adapters
from psycopg.adapt import Dumper
from sqlalchemy import Column
from sqlalchemy.dialects import postgresql as psql


def high_precision_column(precision=None, asdecimal=False, **column_args):
    return Column(
        psql.DOUBLE_PRECISION(precision=precision, asdecimal=asdecimal),
        **column_args
    )


class PathLibDumper(Dumper):
    def dump(self, obj: Any):
        return str(obj).encode("utf-8")


adapters.register_dumper(Path, PathLibDumper)
