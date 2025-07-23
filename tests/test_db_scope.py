"""Test the restored db_scope functionality."""

from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

from lightcurvedb import LCDB_Session
from lightcurvedb.io.pipeline import db_scope
from lightcurvedb.models import Mission


def test_db_scope_import():
    """Test that db_scope can be imported."""
    assert db_scope is not None


def test_db_scope_basic_connection():
    """Test basic database connection through db_scope."""

    @db_scope(application_name="test_db_scope")
    def check_connection(db):
        result = db.execute(text("SELECT 1")).scalar()
        return result

    result = check_connection()
    assert result == 1


def test_db_scope_default_session_factory():
    """Test that db_scope uses LCDB_Session by default."""

    @db_scope()
    def check_default_factory(db):
        # Should be able to query using the default session
        return db.execute(text("SELECT current_database()")).scalar()

    result = check_default_factory()
    assert result is not None  # Should return the database name


def test_db_scope_model_query():
    """Test querying models through db_scope."""

    @db_scope()
    def count_missions(db):
        return db.query(Mission).count()

    count = count_missions()
    assert isinstance(count, int)
    assert count >= 0


def test_db_scope_rollback():
    """Test that uncommitted changes are rolled back."""

    @db_scope()
    def get_initial_count(db):
        return db.query(Mission).count()

    initial_count = get_initial_count()

    @db_scope(application_name="test_rollback")
    def add_mission_without_commit(db):
        new_mission = Mission(
            name="TEST_MISSION_ROLLBACK",
            description="This should be rolled back",
        )
        db.add(new_mission)
        # Not committing, so this should be rolled back

    add_mission_without_commit()

    @db_scope()
    def verify_rollback(db):
        return (
            db.query(Mission).filter_by(name="TEST_MISSION_ROLLBACK").first()
        )

    result = verify_rollback()
    assert result is None  # Mission should not exist due to rollback

    final_count = get_initial_count()
    assert final_count == initial_count  # Count should be unchanged


def test_db_scope_custom_session_factory():
    """Test using a custom session factory."""
    # Note: This test assumes you have a test database available
    # In a real test suite, you'd use a test-specific database

    # For this test, we'll use the same engine as LCDB_Session
    # but demonstrate that a custom factory works
    custom_factory = sessionmaker(bind=LCDB_Session.kw.get("bind"))

    @db_scope(session_factory=custom_factory)
    def check_custom_factory(db):
        return db.execute(text("SELECT 1")).scalar()

    result = check_custom_factory()
    assert result == 1


def test_db_scope_session_kwargs():
    """Test passing kwargs to the session factory."""

    @db_scope(info={"test_key": "test_value"})
    def check_session_info(db):
        # Session info should contain our custom data
        return db.info.get("test_key")

    result = check_session_info()
    assert result == "test_value"
