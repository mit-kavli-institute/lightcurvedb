import pytest
import sys
import decorator
from lightcurvedb.core.connection import db_from_config
from lightcurvedb.core.base_model import QLPModel
from .constants import CONFIG_PATH

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

@pytest.yield_fixture(scope='function')
def db_session(db_conn):
    db_conn.session.begin_nested()
    yield db_conn
    db_conn.session.rollback()
