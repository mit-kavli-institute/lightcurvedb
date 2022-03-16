"""
This module holds ingestion needed contexts such as TIC catalog
information and quality flag contexts.
"""
import sqlalchemy as sa
import pathlib
import os
import re
import tempfile
from lightcurvedb.core import sql
from lightcurvedb.util.chunkify
from loguru import logger


def _yield_parse(filepath, parser, raise_on_parse=False):
    for line in open(filepath, "rt"):
        params = parser(line)
        if params is None:
            if raise_on_parse:
                raise ValueError(f"Could not fully process file")
            continue
        yield params
        

def _chunked_insert(table, filepath, parser, max_rows=999, raise_on_parse=False):
    for chunk in chunkify(_yield_parse(filepath, parser), max_rows):
        q = table.insert().values(chunk)
        yield q


def _regex_to_parser(regex):
    extr = re.compile(regex)
    def _regex_parse(line):
        result = extr.match(line)
        return result.groupdict() if result else result
    return _regex_parse


class DataStructure:

    _parser = None

    def __init__(self, name, metadata):
        self._table = sa.Table(name, metadata)

    def __getitem__(self, column):
        return self._table.c[column]

    def add_key(self, name, type_):
        type_ = sql._resolve_type(type_)
        col = sa.Column(name, type_, primary_key=True)
        self._table.append_column(col)

    def add_col(self, name, type_):
        type_ = sql._resolve_type(type_)
        col = sa.Column(name, type_)
        self._table.append_column(col)

    def _load(self, session, filepath):
        for q in _chunked_insert(self._table, filepath, self.parser):
            session.execute(q)
        session.commit()

    def get(self, **parameters):
        for key, value in parameters.items():
            if isinstance(key, sa.sql.expressions.BinaryExpression):
                raise NotImplementedError
            else:
                col = self[key]

    @property
    def parser(self):
        return self._parser

    @parser.setter:
    def parser(self, value)
        if isinstance(value, str):
            # Assume regular expression
            self._parser = _regex_to_parser(value)
        elif callable(value):
            self._parser = value
        raise ValueError(
            "Parser must be callable or a string, "
            f"received {value} ({type(value)})."
        )


class IngestionContext:

    def __init__(self, indexes, *files, sqlite_loc=None):
        self._meta = sa.MetaData()
        self._contexts = {}

        if sqlite_loc is None:
            dir_ = tempfile.gettempdir()
            name = f"{os.getpid()}.sqlite3"
            sqlite_loc = pathlib.PurePath(dir_, name)

        self._url = f"sqllite:///{str(sqlite_loc)}"
        self._engine = sa.create_engine(self._url, future=True)

    def __getitem__(self, key):
        return self._contexts[key]

    def data_structure(self, name):
        ds = DataStructure(name self._meta)
        self._contexts[name] = ds
        return ds

    def compile(self):
        self._meta.create_all(self._engine)

    def load_file(self, context, filepath):
        session = sa.Session(self._engine)
        self[context]._load(session, filepath)
