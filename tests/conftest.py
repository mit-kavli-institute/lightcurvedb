import os
import pathlib
import time
from tempfile import TemporaryDirectory

import numpy as np
import pytest
import sqlalchemy as sa
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker

from lightcurvedb.core.base_model import LCDBModel
from lightcurvedb.core.connection import LCDB_Session
from lightcurvedb.models import (
    Instrument,
    Mission,
    MissionCatalog,
    Observation,
    PhotometricSource,
    ProcessingMethod,
    Target,
)

from .util import drop_unmanaged_relations, partitioned_table_names


def get_test_database_name(request):
    """Get a unique database name for the test session.

    Uses pytest-xdist worker ID if available, otherwise uses 'master'.
    """
    # Check if we're running under xdist
    worker_id = getattr(request.config, "workerinput", {}).get(
        "workerid", "master"
    )
    return f"lcdb_test_{worker_id}"


@pytest.fixture(scope="session")
def worker_database(request):
    """Create a database for this test worker for the entire session."""
    # Database connection parameters
    db_host = os.environ.get("POSTGRES_HOST", "localhost")
    db_port = int(os.environ.get("POSTGRES_PORT", "5432"))
    db_user = os.environ.get("POSTGRES_USER", "postgres")
    db_password = os.environ.get("POSTGRES_PASSWORD", "postgres")

    # If we're in Docker, use the service name
    if os.path.exists("/.dockerenv") or os.environ.get("DOCKER_CONTAINER"):
        db_host = "db"

    # Get unique database name for this worker
    db_name = get_test_database_name(request)

    # Connect to postgres database to create our test database
    admin_url = sa.URL.create(
        "postgresql+psycopg",
        database="postgres",
        username=db_user,
        password=db_password,
        host=db_host,
        port=db_port,
    )

    admin_engine = sa.create_engine(admin_url, poolclass=sa.pool.NullPool)

    # Create the test database
    with admin_engine.connect().execution_options(
        isolation_level="AUTOCOMMIT"
    ) as conn:
        # Drop if exists (in case of previous unclean shutdown)
        conn.execute(sa.text(f"DROP DATABASE IF EXISTS {db_name}"))
        conn.execute(sa.text(f"CREATE DATABASE {db_name}"))

    # Yield the database configuration
    yield {
        "name": db_name,
        "host": db_host,
        "port": db_port,
        "user": db_user,
        "password": db_password,
    }

    # Cleanup: Drop the database
    with admin_engine.connect().execution_options(
        isolation_level="AUTOCOMMIT"
    ) as conn:
        # First, terminate all connections to the database
        conn.execute(
            sa.text(
                f"""
            SELECT pg_terminate_backend(pg_stat_activity.pid)
            FROM pg_stat_activity
            WHERE pg_stat_activity.datname = '{db_name}'
            AND pid != pg_backend_pid()
        """
            )
        )
        # Then drop the database
        conn.execute(sa.text(f"DROP DATABASE IF EXISTS {db_name}"))

    admin_engine.dispose()


@pytest.fixture
def database_engine(worker_database):
    """Engine bound to the worker database, with the schema created.

    Tears down by sweeping unmanaged relations and then dropping the
    mapped tables, so each test starts from a known-empty schema.
    """
    url = sa.URL.create(
        "postgresql+psycopg",
        database=worker_database["name"],
        username=worker_database["user"],
        password=worker_database["password"],
        host=worker_database["host"],
        port=worker_database["port"],
    )

    # Create engine with retry logic for database connection
    engine = None
    max_retries = 30  # 30 seconds total timeout
    retry_interval = 1  # 1 second between retries

    for attempt in range(max_retries):
        try:
            engine = sa.create_engine(url, poolclass=sa.pool.NullPool)
            # Test the connection
            with engine.connect() as conn:
                conn.execute(sa.text("SELECT 1"))
            break
        except OperationalError as e:
            if attempt < max_retries - 1:
                print(
                    f"Database connection attempt {attempt + 1}/"
                    f"{max_retries} failed. Retrying in {retry_interval}s..."
                )
                time.sleep(retry_interval)
            else:
                raise Exception(
                    f"Could not connect to database "
                    f"{worker_database['name']} at "
                    f"{worker_database['host']}: {worker_database['port']} "
                    f"after {max_retries} attempts"
                ) from e

    LCDBModel.metadata.create_all(bind=engine, checkfirst=True)

    try:
        yield engine
    finally:
        drop_unmanaged_relations(engine)
        LCDBModel.metadata.drop_all(bind=engine)
        engine.dispose()


@pytest.fixture
def default_partitions(database_engine):
    """Catch-all DEFAULT partitions for every partitioned table.

    Lets a test insert into a partitioned table without provisioning a
    partition for the key first.

    .. warning::
       A DEFAULT partition makes every subsequent ``ATTACH PARTITION``
       scan it under ``ACCESS EXCLUSIVE`` to prove no row belongs to the
       incoming bound, and makes the attach fail outright if one does. It
       also forbids ``DETACH PARTITION ... CONCURRENTLY``. Partition
       management tests should use :func:`partitioned_db` instead.
    """
    with database_engine.connect() as conn:
        for table in partitioned_table_names():
            conn.execute(
                sa.text(
                    f"CREATE TABLE IF NOT EXISTS {table}_default "
                    f"PARTITION OF {table} DEFAULT"
                )
            )
        conn.commit()


@pytest.fixture
def _bound_session(database_engine):
    """Session on the worker database, with the id=0 sentinels present.

    Also rebinds the global ``LCDB_Session`` so code reaching for the
    module-level session lands on the test database.
    """
    Session = sessionmaker()
    Session.configure(bind=database_engine)

    # Configure global lightcurvedb sessionmaker
    LCDB_Session.configure(bind=database_engine)

    session = Session()

    # Create sentinel records for composite key support
    PhotometricSource.get_or_create_unspecified(session)
    ProcessingMethod.get_or_create_unspecified(session)
    session.commit()

    try:
        yield session
    finally:
        session.close()


@pytest.fixture
def v2_db(_bound_session, default_partitions):
    """Schema, DEFAULT partitions and sentinels -- the general-purpose DB.

    Both arguments depend on ``database_engine``, which runs
    ``create_all``, so the partitioned parents exist before
    ``default_partitions`` runs regardless of argument order.
    """
    return _bound_session


@pytest.fixture
def partitioned_db(_bound_session, request):
    """Schema and sentinels, deliberately *without* DEFAULT partitions.

    For partition-management tests, where a DEFAULT partition would make
    ``ATTACH`` scan it -- or fail outright. Provision real partitions
    explicitly instead.

    Refuses to be composed with ``default_partitions``, including
    transitively. The realistic way that happens is a module requesting
    ``partitioned_db`` without overriding ``orm_session``: the ``sample_*``
    graph then resolves through ``v2_db`` and creates the defaults anyway,
    silently defeating this fixture.
    """
    if "default_partitions" in request.fixturenames:
        pytest.fail(
            "partitioned_db was requested alongside default_partitions "
            "(directly, or via v2_db / the sample_* fixtures). Override "
            "orm_session in this module to point at partitioned_db."
        )
    return _bound_session


@pytest.fixture
def orm_session(v2_db):
    """Session the ``sample_*`` fixtures build against.

    Indirection so a module can point the shared object graph at a
    different database without redefining every fixture::

        @pytest.fixture
        def orm_session(partitioned_db):
            return partitioned_db
    """
    return v2_db


@pytest.fixture
def sample_mission(orm_session) -> Mission:
    """Create a sample mission for tests."""
    mission = Mission(
        name="Test Mission",
        description="A test mission",
        time_epoch=2457000,
        time_epoch_scale="tdb",
        time_epoch_format="jd",
        time_format_name="test_time",
    )
    orm_session.add(mission)
    orm_session.flush()
    return mission


@pytest.fixture
def sample_catalog(orm_session, sample_mission) -> MissionCatalog:
    """Create a sample catalog for tests."""
    catalog = MissionCatalog(
        name="Test Catalog",
        description="A test catalog",
        host_mission=sample_mission,
    )
    orm_session.add(catalog)
    orm_session.flush()
    return catalog


@pytest.fixture
def sample_target(orm_session, sample_catalog) -> Target:
    """Create a sample target for tests."""
    target = Target(catalog=sample_catalog, name=123456789)
    orm_session.add(target)
    orm_session.flush()
    return target


@pytest.fixture
def sample_instrument(orm_session) -> Instrument:
    """Create a sample instrument for tests."""
    instrument = Instrument(
        name="Test Instrument", properties={"type": "test"}
    )
    orm_session.add(instrument)
    orm_session.flush()
    return instrument


@pytest.fixture
def sample_observation(orm_session, sample_instrument) -> Observation:
    """Create a sample observation for tests."""
    observation = Observation(
        instrument=sample_instrument,
        cadence_reference=np.arange(100),
    )
    orm_session.add(observation)
    orm_session.flush()
    return observation


@pytest.fixture
def sample_photometric_source(orm_session) -> PhotometricSource:
    """Create a named photometric source (not sentinel)."""
    source = PhotometricSource(
        id=100, name="Test Aperture", description="Test aperture"
    )
    orm_session.add(source)
    orm_session.flush()
    return source


@pytest.fixture
def sample_processing_method(orm_session) -> ProcessingMethod:
    """Create a named processing method (not sentinel)."""
    method = ProcessingMethod(
        id=100, name="Test Method", description="Test processing method"
    )
    orm_session.add(method)
    orm_session.flush()
    return method


@pytest.fixture
def tempdir():
    with TemporaryDirectory() as _tmpdir:
        yield pathlib.Path(_tmpdir)


def ensure_directory(path: pathlib.Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    return path
