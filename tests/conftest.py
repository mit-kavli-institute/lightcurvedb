import os
import pathlib
import typing
from tempfile import TemporaryDirectory

import pytest
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker

from lightcurvedb.core.base_model import LCDBModel, QLPModel
from lightcurvedb.core.connection import DB, LCDB_Session
from tests.util import mk_db_config


@pytest.fixture
def db_session():
    url = sa.URL.create(
        "postgresql+psycopg",
        database="postgres",
        username="postgres",
        password="postgres",
        host="db",
        port=5432,
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
def v2_db():
    url = sa.URL.create(
        "postgresql+psycopg",
        database="postgres",
        username="postgres",
        password="postgres",
        host="db",
        port=5432,
    )

    engine = sa.create_engine(url, poolclass=sa.pool.NullPool)

    LCDBModel.metadata.create_all(bind=engine, checkfirst=True)
    Session = sessionmaker()
    Session.configure(bind=engine)

    try:
        yield Session()
    finally:
        LCDBModel.metadata.drop_all(bind=engine)


@pytest.fixture
def tempdir():
    with TemporaryDirectory() as _tmpdir:
        yield pathlib.Path(_tmpdir)


def ensure_directory(path: pathlib.Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


@pytest.fixture
def isolated_database() -> typing.Generator[DB, None, None]:
    admin_url = sa.URL.create(
        "postgresql+psycopg",
        database="postgres",
        username="postgres",
        password="postgres",
        host="db",
        port=5432,
    )

    admin_engine = sa.create_engine(admin_url, poolclass=sa.pool.NullPool)

    pid = os.getpid()
    db_name = f"lcdb_testing_{pid}"
    with admin_engine.connect().execution_options(
        isolation_level="AUTOCOMMIT"
    ) as conn:
        conn.execute(sa.text(f"CREATE DATABASE {db_name}"))
    url = sa.URL.create(
        "postgresql+psycopg",
        database=db_name,
        username="postgres",
        password="postgres",
        host="db",
        port=5432,
    )
    engine = sa.create_engine(url, poolclass=sa.pool.NullPool)
    LCDB_Session.configure(bind=engine)

    try:
        QLPModel.metadata.create_all(bind=engine)
        with LCDB_Session() as temp_session, TemporaryDirectory() as _tempdir:
            config = mk_db_config(
                pathlib.Path(_tempdir),
                database_name=db_name,
                username="postgres",
                password="postgres",
                database_host="db",
                database_port="5432",
            )
            temp_session.config = config
            yield temp_session
            QLPModel.metadata.drop_all(bind=engine)
    finally:
        with admin_engine.connect().execution_options(
            isolation_level="AUTOCOMMIT"
        ) as conn:
            conn.execute(sa.text(f"DROP DATABASE {db_name}"))
