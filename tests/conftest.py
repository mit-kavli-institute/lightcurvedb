import pytest
import pathlib
import tempfile
from click.testing import CliRunner
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import DBAPIError
from functools import wraps
from lightcurvedb import core
from lightcurvedb.core.connection import DB
from lightcurvedb.core.engines import engine_from_config
from .constants import CONFIG_PATH


class __TEST_DB__(DB):
    @property
    def session(self):
        try:
            return super().session
        except DBAPIError:
            self.session.rollback()
            raise

    def flush(self, *args, **kwargs):
        try:
            return self.session.flush(*args, **kwargs)
        except:
            self.session.rollback()
            raise

    def add(self, *args, **kwargs):
        try:
            val = super().add(*args, **kwargs)
            return val
        except:
            self.session.rollback()
            raise




@pytest.fixture(scope="module")
def db_with_schema():
    _engine = engine_from_config(CONFIG_PATH)
    _factory = sessionmaker(bind=_engine)

    db_ = __TEST_DB__(_factory)
    with db_:
        core.base_model.QLPModel.metadata.create_all(db_.bind)
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
    runner = CliRunner()

    return runner
