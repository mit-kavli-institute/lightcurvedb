import pytest
from sqlalchemy import text
from lightcurvedb.core.connection import db_from_config
from lightcurvedb.core.base_model import QLPModel
from .constants import CONFIG_PATH


def near_equal(a, b, rel_tol=1e-09, abs_tol=0.0):
    return abs(a - b) <= max(rel_tol * max(abs(a), abs(b)), abs_tol)


@pytest.yield_fixture(scope="session")
def db_conn():
    db = db_from_config(CONFIG_PATH).open()
    partition_q = text("CREATE SCHEMA IF NOT EXISTS partitions")
    db.session.execute(partition_q)
    QLPModel.metadata.create_all(db.session.bind)
    db.commit()
    db.close()

    yield db


def clear_all(db):
    for tbl in reversed(QLPModel.metadata.sorted_tables):
        db.session.execute(
            text("TRUNCATE TABLE {} RESTART IDENTITY CASCADE".format(tbl.name))
        )
    db.commit()
