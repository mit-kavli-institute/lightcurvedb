import configparser
import os
import pathlib
from contextlib import contextmanager
from tempfile import TemporaryDirectory
from typing import Generator

import sqlalchemy as sa
from sqlalchemy import pool

from lightcurvedb.core.base_model import LCDBModel
from lightcurvedb.core.connection import LCDB_Session


def import_lc_prereqs(db, lightcurves):
    for lc in lightcurves:
        db.merge(lc.aperture)
        db.merge(lc.lightcurve_type)


def mk_db_config(path: pathlib.Path, **data) -> pathlib.Path:
    config = configparser.ConfigParser()
    config["Credentials"] = data
    config_path = path / "db.conf"
    with open(config_path, "wt") as fout:
        config.write(fout)
    return config_path


@contextmanager
def isolated_database() -> Generator[sa.orm.Session, None, None]:
    # Use env variables with defaults for both Docker and host
    db_host = os.environ.get("POSTGRES_HOST", "localhost")
    db_port = int(os.environ.get("POSTGRES_PORT", "5432"))
    db_user = os.environ.get("POSTGRES_USER", "postgres")
    db_password = os.environ.get("POSTGRES_PASSWORD", "postgres")

    # If we're in Docker, use the service name
    if os.path.exists("/.dockerenv") or os.environ.get("DOCKER_CONTAINER"):
        db_host = "db"

    admin_url = sa.URL.create(
        "postgresql+psycopg",
        database="postgres",
        username=db_user,
        password=db_password,
        host=db_host,
        port=db_port,
    )

    admin_engine = sa.create_engine(admin_url, poolclass=pool.NullPool)

    pid = os.getpid()
    db_name = f"lcdb_testing_{pid}"
    with admin_engine.connect().execution_options(
        isolation_level="AUTOCOMMIT"
    ) as conn:
        conn.execute(sa.text(f"CREATE DATABASE {db_name}"))
    url = sa.URL.create(
        "postgresql+psycopg",
        database=db_name,
        username=db_user,
        password=db_password,
        host=db_host,
        port=db_port,
    )
    engine = sa.create_engine(url, poolclass=sa.pool.NullPool)
    LCDB_Session.configure(bind=engine)

    try:
        LCDBModel.metadata.create_all(bind=engine)
        with LCDB_Session() as temp_session, TemporaryDirectory() as _tempdir:
            config = mk_db_config(
                pathlib.Path(_tempdir),
                database_name=db_name,
                username=db_user,
                password=db_password,
                database_host=db_host,
                database_port=str(db_port),
            )
            temp_session.config = config
            yield temp_session
            LCDBModel.metadata.drop_all(bind=engine)
    finally:
        with admin_engine.connect().execution_options(
            isolation_level="AUTOCOMMIT"
        ) as conn:
            conn.execute(sa.text(f"DROP DATABASE {db_name}"))
