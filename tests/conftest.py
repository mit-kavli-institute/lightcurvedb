import os
import pathlib
import time
from tempfile import TemporaryDirectory

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker

from lightcurvedb.core.base_model import LCDBModel


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
def v2_db(worker_database):
    """Database session fixture that uses the worker-specific database."""
    # Connect to the worker-specific database
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

    # Create tables for this test
    LCDBModel.metadata.create_all(bind=engine, checkfirst=True)
    Session = sessionmaker()
    Session.configure(bind=engine)

    # Configure global lightcurvedb sessionmaker
    from lightcurvedb.core.connection import LCDB_Session

    LCDB_Session.configure(bind=engine)

    try:
        session = Session()
        yield session
        session.close()
    finally:
        # Clean up tables after test
        LCDBModel.metadata.drop_all(bind=engine)
        engine.dispose()


@pytest.fixture
def tempdir():
    with TemporaryDirectory() as _tmpdir:
        yield pathlib.Path(_tmpdir)


def ensure_directory(path: pathlib.Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    return path
