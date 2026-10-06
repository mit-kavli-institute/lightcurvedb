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


def partitioned_table_names() -> tuple[str, ...]:
    """Names of every partitioned table in the shared metadata.

    Derived from the ``postgresql_partition_by`` dialect option rather than
    hardcoded, so a newly partitioned model is picked up without editing
    the fixtures. Read at call time because test modules add models to the
    metadata when they are imported.
    """
    return tuple(
        table.name
        for table in LCDBModel.metadata.sorted_tables
        if table.dialect_kwargs.get("postgresql_partition_by")
    )


def drop_unmanaged_relations(engine: sa.Engine) -> list[str]:
    """Drop public tables that ``metadata.drop_all`` will not, returning them.

    ``drop_all`` only knows about mapped tables. A partition attached to a
    mapped parent is dropped with it, but a *detached* one -- a staging or
    retired partition left behind by a partition-management test -- is an
    ordinary standalone table and survives. Partition names are
    deterministic, so the next test in the same worker database would hit
    "relation already exists"; worse, a leftover carrying a foreign key to
    ``observation`` makes ``DROP TABLE observation`` fail and cascades into
    unrelated failures.

    The managed set is read at call time, not at import: some test modules
    declare models against the shared metadata when imported, so a set
    captured earlier would classify them as unmanaged and drop them.
    """
    managed = set(LCDBModel.metadata.tables)
    quote = engine.dialect.identifier_preparer.quote
    with engine.connect() as conn:
        leftovers = [
            name
            for name in conn.execute(
                sa.text(
                    "SELECT c.relname FROM pg_class c "
                    "JOIN pg_namespace n ON n.oid = c.relnamespace "
                    "WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p')"
                )
            ).scalars()
            if name not in managed
        ]
        for name in leftovers:
            conn.execute(
                sa.text(f"DROP TABLE IF EXISTS {quote(name)} CASCADE")
            )
        conn.commit()
    return leftovers


#: A test-only partitioned table whose foreign keys point at the
#: partitioned ``dataset``. The schema no longer has such a table, but the
#: partition package must still handle one: a key onto a partitioned
#: referent cannot be mirrored onto a staging table (§8.1), and its
#: presence is what forces ``plan_swap`` to order a multi-table swap.
#: Created with plain DDL rather than a model so it never enters the
#: shared metadata; :func:`drop_unmanaged_relations` sweeps it.
DATASET_LINK = "datasetlink"

_KEY_COLUMNS = (
    "observation_id",
    "target_id",
    "photometric_method_id",
    "processing_method_id",
)


def create_dataset_link(engine: sa.Engine) -> str:
    """Create :data:`DATASET_LINK`, returning its name."""
    keys = ", ".join(_KEY_COLUMNS)
    source = ", ".join(f"source_{c}" for c in _KEY_COLUMNS)
    child = ", ".join(f"child_{c}" for c in _KEY_COLUMNS)
    columns = [
        f"{side}_{column} {'bigint' if column == 'target_id' else 'integer'}"
        " NOT NULL"
        for side in ("source", "child")
        for column in _KEY_COLUMNS
    ]
    constraints = [
        f"CONSTRAINT pk_{DATASET_LINK} PRIMARY KEY ({source}, {child})",
        f"CONSTRAINT fk_{DATASET_LINK}_source FOREIGN KEY ({source}) "
        f"REFERENCES dataset ({keys}) ON DELETE CASCADE",
        f"CONSTRAINT fk_{DATASET_LINK}_child FOREIGN KEY ({child}) "
        f"REFERENCES dataset ({keys}) ON DELETE CASCADE",
        f"CONSTRAINT ck_{DATASET_LINK}_intra_orbit "
        "CHECK (source_observation_id = child_observation_id)",
    ]
    body = ", ".join(columns + constraints)
    with engine.connect() as conn:
        conn.execute(
            sa.text(
                f"CREATE TABLE {DATASET_LINK} ({body}) "
                "PARTITION BY LIST (source_observation_id)"
            )
        )
        for side, columns_ in (("source", source), ("child", child)):
            conn.execute(
                sa.text(
                    f"CREATE INDEX ix_{DATASET_LINK}_{side} "
                    f"ON {DATASET_LINK} ({columns_})"
                )
            )
        conn.commit()
    return DATASET_LINK
