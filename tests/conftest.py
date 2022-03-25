import pytest
from lightcurvedb import db_from_config, core
from .constants import CONFIG_PATH


@pytest.fixture(scope="module")
def db_with_schema():
    db_ = db_from_config(CONFIG_PATH)
    with db_:
        core.base_model.QLPModel.meta.create_all(db_.dbin)
    return db


@pytest.fixture
def db(db_with_schema):
    with db_with_schema as db_:
        yield db_
        db_.rollback()
