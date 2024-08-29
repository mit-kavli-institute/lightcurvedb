import pathlib
import tempfile

import pytest
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker

from lightcurvedb.core.base_model import QLPModel
from lightcurvedb.core.connection import DB


@pytest.fixture
def db_session(postgresql):
    url = sa.URL.create(
        "postgresql+psycopg",
        database=postgresql.info.dbname,
        username=postgresql.info.user,
        password=postgresql.info.password,
        host=postgresql.info.host,
        port=postgresql.info.port,
    )

    engine = sa.create_engine(url, poolclass=sa.pool.NullPool)

    QLPModel.metadata.create_all(bind=engine, checkfirst=True)
    Session = sessionmaker(class_=DB)

    Session.configure(bind=engine)

    sess = Session()

    try:
        yield sess
    finally:
        sess.close()


@pytest.fixture
def tempdir():
    with tempfile.TemporaryDirectory() as _tmpdir:
        yield pathlib.Path(_tmpdir)


def ensure_directory(path: pathlib.Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    return path
