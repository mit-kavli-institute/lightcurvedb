import pytest
import sys
import decorator
from lightcurvedb.core.connection import db_from_config
from lightcurvedb.core.base_model import QLPModel
from .constants import CONFIG_PATH


def near_equal(a, b, rel_tol=1e-09, abs_tol=0.0):
    return abs(a-b) <= max(rel_tol * max(abs(a), abs(b)), abs_tol)


@pytest.yield_fixture(scope='session')
def db_conn():
    db = db_from_config(CONFIG_PATH)
    QLPModel.metadata.create_all(db._engine)
    try:
        yield db.open()
    except Exception:
        db.close()
        raise
    db.close()

def clear_all():
    for tbl in reversed(QLPModel.metadata.sorted_tables):
        yield tbl.delete()
