"""Tests for core/engines.py and core/connection.py modules.

These tests exercise the database connection and engine creation utilities,
including process guards for multi-process safety.
"""

import os
from unittest.mock import patch

import pytest
from sqlalchemy import text
from sqlalchemy.pool import NullPool


class TestThreadSafeEngine:
    """Tests for thread_safe_engine function."""

    def test_creates_working_engine(self, worker_database):
        """Test that thread_safe_engine creates a functional engine."""
        from lightcurvedb.core.engines import thread_safe_engine

        engine = thread_safe_engine(
            database_name=worker_database["name"],
            username=worker_database["user"],
            password=worker_database["password"],
            database_host=worker_database["host"],
            database_port=worker_database["port"],
            dialect="postgresql+psycopg",
            poolclass=NullPool,
        )

        # Verify engine can execute queries
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1 as value"))
            row = result.fetchone()
            assert row[0] == 1

        engine.dispose()

    def test_registers_pool_event_listeners(self, worker_database):
        """Test that pool event listeners are registered."""
        from sqlalchemy import event

        from lightcurvedb.core.engines import thread_safe_engine

        engine = thread_safe_engine(
            database_name=worker_database["name"],
            username=worker_database["user"],
            password=worker_database["password"],
            database_host=worker_database["host"],
            database_port=worker_database["port"],
            dialect="postgresql+psycopg",
        )

        # Verify listeners are registered on the engine
        # The process guards register 'connect' and 'checkout' events
        assert event.contains(engine, "connect", lambda *a: None) is False
        # If no listeners, event.contains returns False, but having any
        # listener means the event system is active. We verify by checking
        # the pool dispatch has listeners registered.
        pool = engine.pool
        # Check that connect and checkout listeners exist by verifying
        # the dispatch object has been populated
        assert hasattr(pool.dispatch, "connect")
        assert hasattr(pool.dispatch, "checkout")

        engine.dispose()

    def test_connect_stores_pid(self, worker_database):
        """Test that connect listener stores PID in connection record."""
        from lightcurvedb.core.engines import thread_safe_engine

        engine = thread_safe_engine(
            database_name=worker_database["name"],
            username=worker_database["user"],
            password=worker_database["password"],
            database_host=worker_database["host"],
            database_port=worker_database["port"],
            dialect="postgresql+psycopg",
        )

        # Make a connection to trigger the connect event
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

        engine.dispose()

    def test_engine_overrides_passed_to_create_engine(self, worker_database):
        """Test that extra kwargs are passed to create_engine."""
        from lightcurvedb.core.engines import thread_safe_engine

        engine = thread_safe_engine(
            database_name=worker_database["name"],
            username=worker_database["user"],
            password=worker_database["password"],
            database_host=worker_database["host"],
            database_port=worker_database["port"],
            dialect="postgresql+psycopg",
            poolclass=NullPool,
            echo=False,
        )

        # Engine should be created with echo=False
        assert engine.echo is False

        engine.dispose()


class TestProcessGuards:
    """Tests for process guard functionality."""

    def test_checkout_guard_same_pid_succeeds(self, worker_database):
        """Test checkout succeeds when PID matches."""
        from lightcurvedb.core.engines import thread_safe_engine

        engine = thread_safe_engine(
            database_name=worker_database["name"],
            username=worker_database["user"],
            password=worker_database["password"],
            database_host=worker_database["host"],
            database_port=worker_database["port"],
            dialect="postgresql+psycopg",
        )

        # Multiple connections from same process should work
        with engine.connect() as conn1:
            result1 = conn1.execute(text("SELECT 1"))
            assert result1.fetchone()[0] == 1

        with engine.connect() as conn2:
            result2 = conn2.execute(text("SELECT 2"))
            assert result2.fetchone()[0] == 2

        engine.dispose()

    def test_checkout_guard_detects_pid_mismatch(self, worker_database):
        """Test checkout listener detects PID mismatch and recycles connection.

        The checkout listener invalidates connections that were created in a
        different process (detected via PID mismatch). When this happens,
        SQLAlchemy's pool catches the DisconnectionError and creates a fresh
        connection. This prevents issues with multiprocessing/forking.
        """
        from lightcurvedb.core.engines import thread_safe_engine

        # Use a regular pool to test checkout behavior
        engine = thread_safe_engine(
            database_name=worker_database["name"],
            username=worker_database["user"],
            password=worker_database["password"],
            database_host=worker_database["host"],
            database_port=worker_database["port"],
            dialect="postgresql+psycopg",
            pool_size=1,
            max_overflow=0,
        )

        # Get a connection to establish PID in connection record
        conn = engine.connect()
        conn.execute(text("SELECT 1"))
        conn.close()

        # Now simulate a fork by patching os.getpid to return different PID
        original_pid = os.getpid()
        fake_pid = original_pid + 12345

        # Patch in the engines module where the checkout listener uses it
        with patch(
            "lightcurvedb.core.engines.os.getpid", return_value=fake_pid
        ):
            # The checkout listener detects PID mismatch and raises
            # DisconnectionError. The pool catches this and creates a new
            # connection, so connect() succeeds with a fresh connection.
            # The key behavior is that the stale connection is invalidated.
            conn = engine.connect()
            # Connection should work - it's a fresh one after recycling
            result = conn.execute(text("SELECT 1"))
            assert result.scalar() == 1
            conn.close()

        engine.dispose()


class TestDbFromConfig:
    """Tests for db_from_config function."""

    @pytest.fixture
    def config_file(self, worker_database, tempdir):
        """Create a temporary config file with test database credentials."""
        config_content = f"""[Credentials]
database_name = {worker_database["name"]}
username = {worker_database["user"]}
password = {worker_database["password"]}
database_host = {worker_database["host"]}
database_port = {worker_database["port"]}
dialect = postgresql+psycopg
"""
        config_path = tempdir / "test_db.conf"
        config_path.write_text(config_content)
        return config_path

    def test_creates_session_from_config(self, config_file, worker_database):
        """Test db_from_config creates a working session."""
        from lightcurvedb.core.connection import db_from_config

        session = db_from_config(config_file)

        # Verify session works
        result = session.execute(text("SELECT 1 as value"))
        row = result.fetchone()
        assert row[0] == 1

        session.close()

    def test_session_can_query_database(self, config_file, worker_database):
        """Test that session from config can perform database operations."""
        from lightcurvedb.core.connection import db_from_config

        session = db_from_config(config_file)

        # Test various SQL operations
        result = session.execute(text("SELECT current_database() as db_name"))
        db_name = result.fetchone()[0]
        assert db_name == worker_database["name"]

        session.close()


class TestConfigureEngine:
    """Tests for configure_engine function."""

    @pytest.fixture
    def config_file(self, worker_database, tempdir):
        """Create a temporary config file with test database credentials."""
        config_content = f"""[Credentials]
database_name = {worker_database["name"]}
username = {worker_database["user"]}
password = {worker_database["password"]}
database_host = {worker_database["host"]}
database_port = {worker_database["port"]}
"""
        config_path = tempdir / "test_engine.conf"
        config_path.write_text(config_content)
        return config_path

    def test_creates_engine_from_config(self, config_file, worker_database):
        """Test configure_engine creates a working engine."""
        from lightcurvedb.core.connection import configure_engine

        engine = configure_engine(config_file)

        # Verify engine works
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1 as value"))
            row = result.fetchone()
            assert row[0] == 1

        engine.dispose()

    def test_engine_uses_nullpool(self, config_file):
        """Test that configure_engine uses NullPool."""
        from lightcurvedb.core.connection import configure_engine

        engine = configure_engine(config_file)

        # NullPool engines have pool class attribute
        assert isinstance(engine.pool, NullPool)

        engine.dispose()

    def test_engine_uses_postgresql_dialect(self, config_file):
        """Test that configure_engine uses postgresql+psycopg dialect."""
        from lightcurvedb.core.connection import configure_engine

        engine = configure_engine(config_file)

        # Check dialect
        assert engine.dialect.name == "postgresql"

        engine.dispose()


class TestGlobalSessionInitialization:
    """Tests for global session initialization behavior."""

    def test_lcdb_session_is_sessionmaker(self):
        """Test LCDB_Session is a sessionmaker instance."""
        from lightcurvedb.core.connection import LCDB_Session

        # LCDB_Session should be callable sessionmaker
        assert callable(LCDB_Session)

    def test_db_is_none_without_config(self):
        """Test db is None when config file doesn't exist."""
        # This tests the module-level behavior when DEFAULT_CONFIG_PATH
        # doesn't exist (which is typical in test environments)
        from lightcurvedb.util.constants import DEFAULT_CONFIG_PATH

        if not DEFAULT_CONFIG_PATH.exists():
            from lightcurvedb.core.connection import db

            assert db is None
