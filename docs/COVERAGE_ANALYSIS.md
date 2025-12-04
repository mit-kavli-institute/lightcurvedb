# Test Coverage Analysis

**Generated**: 2025-12-03
**Updated**: 2025-12-04
**Overall Coverage**: **98%** (359 statements, 6 missing)
**Test Framework**: pytest + hypothesis
**Database**: PostgreSQL (via pytest-postgresql)

## Summary

| Module | Coverage | Missing Lines | Status |
|--------|----------|---------------|--------|
| `models/instrument.py` | **100%** | — | ✅ Addressed (PR #20) |
| `models/target.py` | **100%** | — | ✅ Addressed (PR #21) |
| `io/pipeline/scope.py` | **100%** | — | ✅ Addressed (PR #24) |
| `core/connection.py` | **93%** | 78-79 | ✅ Addressed (PR #22) |
| `core/engines.py` | **95%** | 20 | ✅ Addressed (PR #22) |
| `exceptions.py` | — | — | ✅ Removed (PR #23) |
| `core/types.py` | 97% | 212 | Low priority |
| `util/iter.py` | 92% | 40, 82 | Low priority |

---

## Remaining Coverage Gaps

### 1. `core/connection.py` (93% coverage)

**Missing Lines**: 78-79

```python
# Lines 78-79: Global session initialization (only runs when config exists)
LCDB_Session.configure(bind=configure_engine(DEFAULT_CONFIG_PATH))
db = LCDB_Session()
```

**Why It's Not Covered**: This code only executes at module import time when `~/.config/lightcurvedb/db.conf` exists. Test environments don't have this file.

**Status**: Acceptable - This is production initialization code that can't be easily tested without side effects.

---

### 2. `core/engines.py` (95% coverage)

**Missing Lines**: 20

```python
# Line 20: DisconnectionError raise in checkout guard
raise DisconnectionError("Attempting to disassociate database connection")
```

**Why It's Not Covered**: SQLAlchemy 2.x made `connection_record.connection` read-only, so line 19 raises `AttributeError` before reaching line 20. The connection is still rejected (test passes), just with a different error type.

**Status**: Known SQLAlchemy 2.x compatibility issue - the safety mechanism works, error type differs.

---

### 3. `core/types.py` (97% coverage)

**Missing Lines**: 212

```python
# Line 212: Fallback return in _get_numpy_dtype
return None  # Default to let numpy infer the dtype
```

**Why It's Not Covered**: All model fields use SQL types that are in the type map. The fallback for unsupported types is never reached.

**Quick Fix**:
```python
def test_numpy_array_type_unsupported():
    from sqlalchemy import String
    from lightcurvedb.core.types import NumpyArrayType
    arr = NumpyArrayType(String)
    assert arr._get_numpy_dtype() is None
```

---

### 4. `util/iter.py` (92% coverage)

**Missing Lines**: 40, 82

```python
# Line 40: ValueError for invalid chunksize
if chunksize < 1:
    raise ValueError("Chunkify command cannot have a chunksize < 1")

# Line 82: ValueError for invalid partition count
if n < 1:
    raise ValueError("Number of partitions must be at least 1")
```

**Why It's Not Covered**: Tests use valid parameters; error paths aren't exercised.

**Quick Fix**:
```python
import pytest
from lightcurvedb.util.iter import chunkify, eq_partitions

def test_chunkify_invalid_size():
    with pytest.raises(ValueError, match="chunksize < 1"):
        list(chunkify([1, 2, 3], 0))

def test_eq_partitions_invalid_n():
    with pytest.raises(ValueError, match="at least 1"):
        eq_partitions([1, 2, 3], 0)
```

---

## Addressed Issues

### ✅ PR #20: `models/instrument.py`
- **Coverage**: 74% → **100%**
- **Tests Added**: `TestQueryForInstrument` class in `tests/test_instrument.py`
- **Lines Covered**: 80-88 (`query_for_instrument` method)

### ✅ PR #21: `models/target.py`
- **Coverage**: 84% → **100%**
- **Tests Added**: `TestMissionTimeEpoch` class in `tests/test_mission.py`
- **Lines Covered**: 73-80 (`register_mission_time_epoch` method)

### ✅ PR #22: `core/connection.py` and `core/engines.py`
- **Coverage**: connection.py 77% → **93%**, engines.py 32% → **95%**
- **Tests Added**: `tests/test_core_connection.py` with 13 tests
- **Approach**: Used docker-compose PostgreSQL with temp config files

### ✅ PR #23: `exceptions.py`
- **Action**: Module removed (unused legacy code)
- **Impact**: Overall coverage improved by removing dead code

### ✅ PR #24: `io/pipeline/scope.py`
- **Coverage**: 62% → **100%**
- **Changes**: Removed unused `scoped_block` context manager, improved type hints
- **Type Hints**: Added `ParamSpec` and `Concatenate` for proper decorator typing

---

## Coverage Progress

| Date | Overall | Notes |
|------|---------|-------|
| 2025-12-03 | 84% | Initial analysis |
| 2025-12-03 | 87% | After PR #20, #21 |
| 2025-12-04 | 92% | After PR #22 |
| 2025-12-04 | 94% | After PR #23 (exceptions removed) |
| 2025-12-04 | **98%** | After PR #24 (scope.py refactored) |

---

## Quick Wins for 100% Coverage

Add these 3 tests to reach 100% coverage:

```python
# tests/test_coverage_gaps.py
import pytest
from lightcurvedb.util.iter import chunkify, eq_partitions

def test_chunkify_invalid_size():
    """Cover util/iter.py line 40."""
    with pytest.raises(ValueError):
        list(chunkify([], 0))

def test_eq_partitions_invalid_n():
    """Cover util/iter.py line 82."""
    with pytest.raises(ValueError):
        eq_partitions([], 0)

def test_numpy_array_type_unsupported():
    """Cover core/types.py line 212."""
    from sqlalchemy import String
    from lightcurvedb.core.types import NumpyArrayType
    arr = NumpyArrayType(String)
    assert arr._get_numpy_dtype() is None
```

This would cover 3 of the 6 remaining lines. The other 3 lines (connection.py:78-79, engines.py:20) are infrastructure code that's difficult to test without side effects or are affected by SQLAlchemy version differences.
