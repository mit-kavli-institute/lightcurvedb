from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2 import sql
import psycopg2
from lightcurvedb import db_from_config, models
from lightcurvedb.core.base_model import QLPModel
import configparser
import pathlib
import os
import tempfile

import pytest
from click.testing import CliRunner


def _db_connection(database):
    conn = psycopg2.connect(
        dbname=database,
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        host=os.environ["HOST"],
        port=os.environ["PORT"]
    )
    return conn


def _create_testdb(testdb_name):
    postgres_conn = _db_connection("postgres")
    postgres_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    with postgres_conn.cursor() as cur:
        drop = (
            sql
            .SQL("DROP DATABASE IF EXISTS {}")
            .format(sql.Identifier(testdb_name))
        )
        create = (
            sql
            .SQL("CREATE DATABASE {}")
            .format(sql.Identifier(testdb_name))
        )
        cur.execute(drop)
        cur.execute(create)
    postgres_conn.close()


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


    db = db_from_config(CONFIG_PATH)
    with db:
        QLPModel.metadata.create_all(db.bind)

    return CONFIG_PATH

TESTNAME = "lightpoint_testing_db"
_create_testdb(TESTNAME)
_populate_configuration(TESTNAME)


@pytest.fixture(scope="module")
def tempdir():
    with tempfile.TemporaryDirectory() as tmp:
        yield pathlib.Path(tmp)


@pytest.fixture(scope="module")
def clirunner():
    runner = CliRunner(mix_stderr=True)

    return runner
