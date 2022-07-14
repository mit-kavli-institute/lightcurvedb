import pathlib
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2 import sql
import psycopg2
import configparser
import os
import tempfile

import pytest
from click.testing import CliRunner
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import sessionmaker

from lightcurvedb.core.engines import engine_from_config

from .constants import CONFIG_PATH

def _db_connection(database):
    conn = psycopg2.connect(
        dbname=database,
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        host=os.envion["HOST"],
        port=os.environ["PORT"]
    )
    return conn


def _create_testdb(testdb_name):
    postgres_conn = _db_connection("postgres")
    postgres_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    with postgres_conn.cursor() as cur:
        q = sql.SQL(f"CREATE DATABASE {sql.Identifier(testdb_name)}")
        cur.execute(q)
    postgress_conn.close()



def _populate_configuration(testdb_name):
    TEST_PATH = os.path.dirname(os.path.relpath(__file__))
    CONFIG_PATH = os.path.join(TEST_PATH, "config.conf")
    parser = configparser.ConfigParser()
    parser["Credentials"] = {
        "username": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
        "database_name": testdb_name,
        "database_host": os.environ["HOST"],
        "database_port": os.environ["PORT"],
    }
    with open(CONFIG_PATH, "wt") as fout:
        parser.write(fout)


@pytest.fixture(scope="module")
def db_with_schema():
    testdb_name = f"lightpoint_testing_{os.getpid()}"
    _create_testdb(testdb_name)
    _populate_configuration(testdb_name)
    _engine = engine_from_config(CONFIG_PATH)
    _factory = sessionmaker(bind=_engine)

    db_ = __TEST_DB__(_factory)
    return db_


@pytest.fixture(scope="module")
def db(db_with_schema):
    with db_with_schema as db_:
        try:
            yield db_
        finally:
            db_.rollback()


@pytest.fixture(scope="module")
def tempdir():
    with tempfile.TemporaryDirectory() as tmp:
        yield pathlib.Path(tmp)


@pytest.fixture(scope="module")
def clirunner():
    runner = CliRunner(mix_stderr=True)

    return runner
