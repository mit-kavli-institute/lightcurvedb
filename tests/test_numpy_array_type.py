"""Tests for the NumpyArrayType custom SQLAlchemy type."""

from typing import Optional

import numpy as np
import pytest
import sqlalchemy as sa
from numpy import typing as npt
from sqlalchemy import orm

from lightcurvedb.core.base_model import LCDBModel
from lightcurvedb.core.types import NumpyArrayType


class ExampleArrayModel(LCDBModel):
    """Test model with numpy array columns."""

    __tablename__ = "test_numpy_arrays"

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)

    # Test all supported types
    int64_array: orm.Mapped[npt.NDArray[np.int64]]
    int32_array: orm.Mapped[npt.NDArray[np.int32]]
    int16_array: orm.Mapped[npt.NDArray[np.int16]]
    int8_array: orm.Mapped[npt.NDArray[np.int8]]
    float64_array: orm.Mapped[npt.NDArray[np.float64]]
    float32_array: orm.Mapped[npt.NDArray[np.float32]]
    bool_array: orm.Mapped[npt.NDArray[np.bool_]]

    # Optional array
    optional_array: orm.Mapped[
        Optional[npt.NDArray[np.float64]]
    ] = orm.mapped_column(nullable=True)


@pytest.fixture(scope="function")
def setup_table(v2_db: orm.Session):
    """Create test table for the session."""
    ExampleArrayModel.metadata.create_all(bind=v2_db.bind)
    yield
    # Ensure clean teardown
    v2_db.rollback()  # Rollback any pending transactions
    ExampleArrayModel.metadata.drop_all(bind=v2_db.bind)


class TestNumpyArrayType:
    """Test suite for NumpyArrayType."""

    @pytest.mark.timeout(5)
    def test_process_bind_param_with_numpy_array(self):
        """Test converting numpy array to list for storage."""
        array_type = NumpyArrayType(sa.Integer)
        arr = np.array([1, 2, 3, 4, 5])

        result = array_type.process_bind_param(arr, None)

        assert isinstance(result, list)
        assert result == [1, 2, 3, 4, 5]

    @pytest.mark.timeout(5)
    def test_process_bind_param_with_list(self):
        """Test that lists pass through unchanged."""
        array_type = NumpyArrayType(sa.Integer)
        lst = [1, 2, 3, 4, 5]

        result = array_type.process_bind_param(lst, None)

        assert result is lst

    @pytest.mark.timeout(5)
    def test_process_bind_param_with_none(self):
        """Test handling None values."""
        array_type = NumpyArrayType(sa.Integer)

        result = array_type.process_bind_param(None, None)

        assert result is None

    @pytest.mark.timeout(5)
    def test_process_result_value_int32(self):
        """Test converting list to numpy array with int32 dtype."""
        array_type = NumpyArrayType(sa.Integer)
        lst = [1, 2, 3, 4, 5]

        result = array_type.process_result_value(lst, None)

        assert isinstance(result, np.ndarray)
        assert result.dtype == np.int32
        np.testing.assert_array_equal(result, np.array([1, 2, 3, 4, 5]))

    @pytest.mark.timeout(5)
    def test_process_result_value_int64(self):
        """Test converting list to numpy array with int64 dtype."""
        array_type = NumpyArrayType(sa.BigInteger)
        lst = [1, 2, 3, 4, 5]

        result = array_type.process_result_value(lst, None)

        assert isinstance(result, np.ndarray)
        assert result.dtype == np.int64
        np.testing.assert_array_equal(
            result, np.array([1, 2, 3, 4, 5], dtype=np.int64)
        )

    @pytest.mark.timeout(5)
    def test_process_result_value_float(self):
        """Test converting list to numpy array with float64 dtype."""
        array_type = NumpyArrayType(sa.Float)
        lst = [1.1, 2.2, 3.3, 4.4, 5.5]

        result = array_type.process_result_value(lst, None)

        assert isinstance(result, np.ndarray)
        assert result.dtype == np.float64
        np.testing.assert_array_almost_equal(
            result, np.array([1.1, 2.2, 3.3, 4.4, 5.5])
        )

    @pytest.mark.timeout(5)
    def test_process_result_value_with_none(self):
        """Test handling None values from database."""
        array_type = NumpyArrayType(sa.Integer)

        result = array_type.process_result_value(None, None)

        assert result is None

    @pytest.mark.timeout(5)
    def test_dtype_mapping(self):
        """Test that SQL types map to correct numpy dtypes."""
        test_cases = [
            (sa.Integer, np.int32),
            (sa.Integer(), np.int32),
            (sa.BigInteger, np.int64),
            (sa.BigInteger(), np.int64),
            (sa.Float, np.float64),
            (sa.Float(), np.float64),
            (sa.SmallInteger, np.int16),
            (sa.SmallInteger(), np.int16),
            (sa.REAL, np.float32),
            (sa.REAL(), np.float32),
            (sa.Boolean, np.bool_),
            (sa.Boolean(), np.bool_),
        ]

        for sql_type, expected_dtype in test_cases:
            array_type = NumpyArrayType(sql_type)
            assert array_type._get_numpy_dtype() == expected_dtype

    @pytest.mark.timeout(5)
    def test_round_trip_conversion(self):
        """Test that data survives a round trip through the type converter."""
        array_type = NumpyArrayType(sa.Float)
        original = np.array([1.1, 2.2, 3.3, 4.4, 5.5], dtype=np.float64)

        # Simulate database round trip
        db_value = array_type.process_bind_param(original, None)
        result = array_type.process_result_value(db_value, None)

        np.testing.assert_array_almost_equal(result, original)
        assert result.dtype == original.dtype

    @pytest.mark.timeout(5)
    def test_int16_processing(self):
        """Test int16 array processing."""
        array_type = NumpyArrayType(sa.SmallInteger)
        arr = np.array([-32768, 0, 32767], dtype=np.int16)

        # Test bind param
        db_value = array_type.process_bind_param(arr, None)
        assert isinstance(db_value, list)

        # Test result value
        result = array_type.process_result_value(db_value, None)
        assert isinstance(result, np.ndarray)
        assert result.dtype == np.int16
        np.testing.assert_array_equal(result, arr)

    @pytest.mark.timeout(5)
    def test_float32_processing(self):
        """Test float32 array processing."""
        array_type = NumpyArrayType(sa.REAL)
        arr = np.array([1.1, 2.2, 3.3], dtype=np.float32)

        # Test bind param
        db_value = array_type.process_bind_param(arr, None)
        assert isinstance(db_value, list)

        # Test result value
        result = array_type.process_result_value(db_value, None)
        assert isinstance(result, np.ndarray)
        assert result.dtype == np.float32
        np.testing.assert_allclose(result, arr, rtol=1e-6)

    @pytest.mark.timeout(5)
    def test_bool_processing(self):
        """Test boolean array processing."""
        array_type = NumpyArrayType(sa.Boolean)
        arr = np.array([True, False, True], dtype=np.bool_)

        # Test bind param
        db_value = array_type.process_bind_param(arr, None)
        assert isinstance(db_value, list)

        # Test result value
        result = array_type.process_result_value(db_value, None)
        assert isinstance(result, np.ndarray)
        assert result.dtype == np.bool_
        np.testing.assert_array_equal(result, arr)


class TestNumpyArrayIntegration:
    """Integration tests with actual database operations."""

    @pytest.mark.timeout(10)
    def test_all_types_roundtrip(self, v2_db: orm.Session, setup_table):
        """Test all supported numpy types in database roundtrip."""
        test_obj = ExampleArrayModel(
            int64_array=np.array([1, 2, 3], dtype=np.int64),
            int32_array=np.array([4, 5, 6], dtype=np.int32),
            int16_array=np.array([7, 8, 9], dtype=np.int16),
            int8_array=np.array([10, 11, 12], dtype=np.int8),
            float64_array=np.array([1.1, 2.2, 3.3], dtype=np.float64),
            float32_array=np.array([4.4, 5.5, 6.6], dtype=np.float32),
            bool_array=np.array([True, False, True], dtype=np.bool_),
        )

        v2_db.add(test_obj)
        v2_db.commit()
        v2_db.refresh(test_obj)

        # Verify all arrays are numpy arrays with correct dtypes
        assert isinstance(test_obj.int64_array, np.ndarray)
        assert test_obj.int64_array.dtype == np.int64

        assert isinstance(test_obj.int32_array, np.ndarray)
        assert test_obj.int32_array.dtype == np.int32

        assert isinstance(test_obj.int16_array, np.ndarray)
        assert test_obj.int16_array.dtype == np.int16

        # int8 is stored as int16 in PostgreSQL
        assert isinstance(test_obj.int8_array, np.ndarray)
        assert test_obj.int8_array.dtype == np.int16

        assert isinstance(test_obj.float64_array, np.ndarray)
        assert test_obj.float64_array.dtype == np.float64

        assert isinstance(test_obj.float32_array, np.ndarray)
        assert test_obj.float32_array.dtype == np.float32

        assert isinstance(test_obj.bool_array, np.ndarray)
        assert test_obj.bool_array.dtype == np.bool_
