# Test Coverage Analysis

**Generated**: 2025-12-03
**Updated**: 2025-12-03
**Overall Coverage**: ~87% (estimated after fixes)
**Test Framework**: pytest + hypothesis
**Database**: PostgreSQL (via pytest-postgresql)

## Summary

| Module | Coverage | Missing Lines | Priority | Status |
|--------|----------|---------------|----------|--------|
| `models/instrument.py` | ~~74%~~ **100%** | ~~80-88~~ | ~~High~~ | ✅ Addressed (PR #20) |
| `models/target.py` | ~~84%~~ **100%** | ~~73-80~~ | ~~Medium~~ | ✅ Addressed (PR #21) |
| `io/pipeline/scope.py` | 62% | 136-153 | Medium | Pending |
| `core/connection.py` | 77% | 37-48, 60-69, 78-79 | Medium | Pending |
| `core/engines.py` | 32% | 11-24, 39-44 | Low | Pending |
| `core/types.py` | 97% | 212 | Low | Pending |
| `exceptions.py` | 0% | 4-31 | Low | Pending |
| `util/iter.py` | 92% | 40, 82 | Low | Pending |

---

## Detailed Analysis

### 1. `core/connection.py` (77% coverage)

**Missing Lines**: 37-48, 60-69, 78-79

#### Context

This module provides database connection utilities using SQLAlchemy and the `configurables` library for configuration file parsing.

**Uncovered Code**:

```python
# Lines 37-48: db_from_config function body
def db_from_config(...):
    engine = thread_safe_engine(...)
    session = sessionmaker(bind=engine)()
    return session

# Lines 60-69: configure_engine function body
def configure_engine(...):
    engine = thread_safe_engine(...)
    return engine

# Lines 78-79: Global session initialization
LCDB_Session.configure(bind=configure_engine(DEFAULT_CONFIG_PATH))
db = LCDB_Session()
```

#### Why It's Not Covered

These functions require a valid `~/.config/lightcurvedb/db.conf` configuration file to execute. The test suite uses `pytest-postgresql` fixtures that create isolated test databases, bypassing these production connection helpers.

#### Recommendation

**Approach**: Integration tests with mock configuration files.

```python
import pytest
from unittest.mock import patch
from pathlib import Path
import tempfile

@pytest.fixture
def mock_config_file():
    """Create a temporary config file for testing."""
    config_content = """
[Credentials]
database_name = test_db
username = test_user
password = test_pass
database_host = localhost
database_port = 5432
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.conf', delete=False) as f:
        f.write(config_content)
        yield Path(f.name)

def test_db_from_config(mock_config_file, postgresql):
    """Test db_from_config creates a valid session."""
    from lightcurvedb.core.connection import db_from_config

    # Patch to use test database connection
    with patch('lightcurvedb.core.engines.create_engine') as mock_engine:
        mock_engine.return_value = postgresql
        session = db_from_config(mock_config_file)
        assert session is not None
```

**Priority**: Medium - These are production utilities, not critical for model testing.

---

### 2. `core/engines.py` (32% coverage)

**Missing Lines**: 11-24, 39-44

#### Context

This module provides SQLAlchemy engine creation with process guards for safe multi-process usage.

**Uncovered Code**:

```python
# Lines 11-24: Process guard event listeners
@listens_for(engine, "connect")
def connect(dbapi_connection, connection_record):
    connection_record.info["pid"] = os.getpid()

@listens_for(engine, "checkout")
def checkout(dbabi_connection, connection_record, connection_proxy):
    pid = os.getpid()
    if connection_record.info["pid"] != pid:
        connection_record.connection = connection_proxy.connection = None
        raise DisconnectionError(...)

# Lines 39-44: thread_safe_engine body
url = f"{dialect}://{username}:{password}@{database_host}:{database_port}/{database_name}"
engine = create_engine(url, **engine_overrides)
return __register_process_guards__(engine)
```

#### Why It's Not Covered

The test suite uses `pytest-postgresql` which creates engines directly, not through `thread_safe_engine`. The process guards are designed for production multi-process scenarios (e.g., multiprocessing workers).

#### Recommendation

**Approach**: Unit tests with mock engines and multi-process simulation.

```python
import os
import pytest
from unittest.mock import MagicMock, patch

def test_thread_safe_engine_registers_guards():
    """Test that process guards are registered on engine creation."""
    from lightcurvedb.core.engines import thread_safe_engine

    with patch('lightcurvedb.core.engines.create_engine') as mock_create:
        mock_engine = MagicMock()
        mock_create.return_value = mock_engine

        engine = thread_safe_engine(
            "testdb", "user", "pass", "localhost", 5432, "postgresql"
        )

        # Verify event listeners were registered
        assert mock_engine.dispatch.connect.append.called or \
               len(mock_engine.dispatch.connect) > 0

def test_checkout_guard_detects_pid_mismatch():
    """Test process guard raises DisconnectionError on PID mismatch."""
    from sqlalchemy.exc import DisconnectionError
    # Simulate checkout from different process
    # This requires mocking connection_record.info["pid"]
```

**Priority**: Low - These are infrastructure guards, not business logic.

---

### 3. `core/types.py` (97% coverage)

**Missing Lines**: 212

#### Context

Custom SQLAlchemy `TypeDecorator` for numpy array handling.

**Uncovered Code**:

```python
# Line 212: Fallback return in _get_numpy_dtype
return None  # Default to let numpy infer the dtype
```

#### Why It's Not Covered

All current model fields use explicit SQL types (Integer, BigInteger, Float, etc.) that match the type map. The fallback is never reached.

#### Recommendation

**Approach**: Property test with unsupported SQL types.

```python
from hypothesis import given
from hypothesis import strategies as st
from sqlalchemy import String, Text
from lightcurvedb.core.types import NumpyArrayType

def test_unsupported_type_returns_none():
    """Test fallback for unsupported SQL types."""
    # String is not in the type map
    array_type = NumpyArrayType(String)
    assert array_type._get_numpy_dtype() is None

@given(st.lists(st.text(min_size=1, max_size=10), min_size=1, max_size=5))
def test_string_array_uses_numpy_inference(values):
    """Test numpy dtype inference for string arrays."""
    import numpy as np
    array_type = NumpyArrayType(String)
    result = array_type.process_result_value(values, None)
    assert result.dtype == np.dtype('O')  # Object dtype for strings
```

**Priority**: Low - Edge case, current usage patterns don't require this.

---

### 4. `exceptions.py` (0% coverage)

**Missing Lines**: 4-31 (entire module)

#### Context

Custom exception classes for lightcurvedb-specific errors.

**Uncovered Code**:

```python
class LightcurveDBException(Exception):
    """Base exception for all manually thrown exceptions."""
    pass

class PrimaryIdentNotFound(LightcurveDBException):
    """Raised when identity does not match any record."""
    pass

class EmptyLightcurve(LightcurveDBException):
    """Raised when a lightcurve has no timeseries data."""
    def __init__(self, q):
        statement = q.compile(dialect=postgresql.dialect())
        super().__init__(f"Could not find any lightcurves with the given context: {statement}")
```

#### Why It's Not Covered

These exceptions are likely from an older codebase version or planned features. The current test suite doesn't exercise code paths that raise them.

#### Recommendation

**Approach**: Test exception instantiation and inheritance.

```python
import pytest
from sqlalchemy import select
from lightcurvedb.exceptions import (
    LightcurveDBException,
    PrimaryIdentNotFound,
    EmptyLightcurve,
)
from lightcurvedb.models import Target

class TestExceptions:
    def test_base_exception_hierarchy(self):
        """Test exception inheritance."""
        assert issubclass(PrimaryIdentNotFound, LightcurveDBException)
        assert issubclass(EmptyLightcurve, LightcurveDBException)
        assert issubclass(LightcurveDBException, Exception)

    def test_primary_ident_not_found_instantiation(self):
        """Test PrimaryIdentNotFound can be raised."""
        with pytest.raises(PrimaryIdentNotFound):
            raise PrimaryIdentNotFound("Target not found")

    def test_empty_lightcurve_message_formatting(self):
        """Test EmptyLightcurve formats query in message."""
        query = select(Target).where(Target.id == 12345)
        exc = EmptyLightcurve(query)
        assert "12345" in str(exc)
        assert "Could not find any lightcurves" in str(exc)
```

**Priority**: Low - Exceptions should be tested when the code that raises them is added.

---

### 5. `io/pipeline/scope.py` (62% coverage)

**Missing Lines**: 136-153

#### Context

Database scope decorators for automatic session management.

**Uncovered Code**:

```python
# Lines 136-153: scoped_block context manager
@contextmanager
def scoped_block(
    db: Session,
    resource: Any,
    acquire_actions: Optional[List[Any]] = None,
    release_actions: Optional[List[Any]] = None,
) -> Any:
    if acquire_actions is None:
        acquire_actions = []
    if release_actions is None:
        release_actions = []

    try:
        for action in acquire_actions:
            logger.trace(action)
            db.execute(action)
        db.commit()
        yield resource
    except InternalError:
        db.rollback()
    finally:
        for action in release_actions:
            logger.trace(action)
            db.execute(action)
        db.commit()
```

#### Why It's Not Covered

This context manager appears to be designed for resource locking patterns (e.g., PostgreSQL advisory locks) but isn't used in the current codebase.

#### Recommendation

**Approach**: Test with PostgreSQL advisory locks or custom SQL actions.

```python
import pytest
from sqlalchemy import text
from lightcurvedb.io.pipeline.scope import scoped_block

class TestScopedBlock:
    def test_scoped_block_basic_yield(self, v2_db):
        """Test basic resource yielding."""
        resource = {"data": "test"}
        with scoped_block(v2_db, resource) as r:
            assert r == resource

    def test_scoped_block_with_acquire_actions(self, v2_db):
        """Test acquire actions are executed."""
        # Use a simple SELECT as acquire action
        acquire = [text("SELECT 1")]
        with scoped_block(v2_db, "resource", acquire_actions=acquire) as r:
            assert r == "resource"

    def test_scoped_block_with_release_actions(self, v2_db):
        """Test release actions are executed in finally block."""
        release = [text("SELECT 1")]
        with scoped_block(v2_db, "resource", release_actions=release):
            pass
        # If we get here without error, release actions executed

    def test_scoped_block_rollback_on_internal_error(self, v2_db):
        """Test InternalError triggers rollback."""
        from sqlalchemy.exc import InternalError
        from unittest.mock import patch, MagicMock

        # Mock to raise InternalError during acquire
        with patch.object(v2_db, 'execute', side_effect=InternalError("test", None, None)):
            with patch.object(v2_db, 'rollback') as mock_rollback:
                try:
                    with scoped_block(v2_db, "resource", acquire_actions=[text("SELECT 1")]):
                        pass
                except:
                    pass
                # Rollback should have been called
```

**Priority**: Medium - Useful for advisory lock patterns in data pipelines.

---

### 6. ✅ `models/instrument.py` — ADDRESSED

> **Status**: Resolved in PR #20
> **Coverage**: 74% → **100%**
> **Tests Added**: `TestQueryForInstrument` class in `tests/test_instrument.py`

<details>
<summary>Original analysis (click to expand)</summary>

**Missing Lines**: 80-88

Query helper method for finding instruments by name and parent.

```python
@classmethod
def query_for_instrument(cls, name: str, parent_name: Optional[str] = None):
    q = sa.select(cls).where(cls.name == name)
    if parent_name:
        parent = orm.aliased(cls)
        q = q.join(cls.parent.of_type(parent)).where(parent.name == parent_name)
    else:
        q = q.where(cls.parent_id.is_(None))
    return q
```

</details>

---

### 7. ✅ `models/target.py` — ADDRESSED

> **Status**: Resolved in PR #21
> **Coverage**: 84% → **100%**
> **Tests Added**: `TestMissionTimeEpoch` class in `tests/test_mission.py`

<details>
<summary>Original analysis (click to expand)</summary>

**Missing Lines**: 73-80

Method to register a custom Astropy time format for mission-specific epochs.

```python
@lru_cache
def register_mission_time_epoch(self):
    class MissionTime(time.TimeEpochDate):
        name = self.time_format_name
        unit = 1 * getattr(u, self.time_unit)
        epoch_val = self.time_epoch
        epoch_scale = self.time_epoch_scale
        epoch_format = self.time_epoch_format

    return MissionTime
```

</details>

---

### 8. `util/iter.py` (92% coverage)

**Missing Lines**: 40, 82

#### Context

Utility functions for chunking and partitioning iterables.

**Uncovered Code**:

```python
# Line 40: ValueError for invalid chunksize
if chunksize < 1:
    raise ValueError("Chunkify command cannot have a chunksize < 1")

# Line 82: ValueError for invalid partition count
if n < 1:
    raise ValueError("Number of partitions must be at least 1")
```

#### Why It's Not Covered

The error paths for invalid inputs aren't exercised - tests use valid parameters.

#### Recommendation

**Approach**: Simple unit tests for error cases.

```python
import pytest
from hypothesis import given
from hypothesis import strategies as st
from lightcurvedb.util.iter import chunkify, eq_partitions

class TestIterErrorCases:
    @given(st.integers(max_value=0))
    def test_chunkify_invalid_chunksize(self, invalid_size):
        """Test chunkify raises for chunksize < 1."""
        with pytest.raises(ValueError, match="chunksize < 1"):
            list(chunkify([1, 2, 3], invalid_size))

    @given(st.integers(max_value=0))
    def test_eq_partitions_invalid_n(self, invalid_n):
        """Test eq_partitions raises for n < 1."""
        with pytest.raises(ValueError, match="at least 1"):
            eq_partitions([1, 2, 3], invalid_n)
```

**Priority**: Low - Simple validation, but easy to add.

---

## Testing Recommendations Summary

### PostgreSQL Testing Patterns

The project uses `pytest-postgresql` for database testing. Key patterns:

```python
# conftest.py provides v2_db fixture
@pytest.fixture
def v2_db(worker_database):
    """Function-scoped session with auto-cleanup."""
    # Creates tables, yields session, drops tables
```

### Hypothesis Integration

Existing patterns in the codebase:

```python
from hypothesis import given, assume
from hypothesis import strategies as st
from hypothesis.extra import numpy as np_st

# Strategy composition
@st.composite
def custom_strategy(draw):
    value = draw(st.integers())
    return transform(value)

# Property testing
@given(data=custom_strategy())
def test_property(self, data):
    # Test invariants
```

### Priority Order for Coverage Improvement

1. **High Priority** (Core functionality):
   - `models/instrument.py:80-88` - Query helper method

2. **Medium Priority** (Integration features):
   - `models/target.py:73-80` - Astropy time registration
   - `io/pipeline/scope.py:136-153` - Scoped block context manager
   - `core/connection.py` - Config-based connections

3. **Low Priority** (Infrastructure/Edge cases):
   - `core/engines.py` - Process guards (multi-process scenarios)
   - `core/types.py:212` - Unsupported type fallback
   - `exceptions.py` - Exception definitions (cover when used)
   - `util/iter.py:40,82` - Error path validation

### Quick Wins

Add these tests for immediate coverage gains:

```python
# tests/test_coverage_gaps.py

def test_chunkify_invalid_size():
    with pytest.raises(ValueError):
        list(chunkify([], 0))

def test_eq_partitions_invalid_n():
    with pytest.raises(ValueError):
        eq_partitions([], 0)

def test_numpy_array_type_unsupported():
    from sqlalchemy import String
    arr = NumpyArrayType(String)
    assert arr._get_numpy_dtype() is None
```

This adds 3 lines of coverage with minimal effort.
