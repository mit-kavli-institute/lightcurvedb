"""Tests for DataSet.align_to_observation method."""

import numpy as np
import pytest
from hypothesis import assume, given
from hypothesis import strategies as st
from hypothesis.extra import numpy as np_st

from lightcurvedb.models import DataSet, Observation


# Reuse strategies from test_observation.py
@st.composite
def monotonic_int64_array(draw, min_size=0, max_size=100):
    """Generate sorted unique int64 arrays (like cadence_reference)."""
    size = draw(st.integers(min_value=min_size, max_value=max_size))
    if size == 0:
        return np.array([], dtype=np.int64)
    elements = draw(
        st.lists(
            st.integers(min_value=0, max_value=10000),
            min_size=size,
            max_size=size,
            unique=True,
        )
    )
    return np.array(sorted(elements), dtype=np.int64)


@st.composite
def reference_and_valid_subset(draw, min_ref_size=1, max_ref_size=50):
    """Generate (reference, observed, values) where observed ⊆ reference."""
    reference = draw(
        monotonic_int64_array(min_size=min_ref_size, max_size=max_ref_size)
    )
    # Select random subset of indices
    n_observed = draw(st.integers(min_value=0, max_value=len(reference)))
    indices = draw(
        st.lists(
            st.sampled_from(range(len(reference))),
            min_size=n_observed,
            max_size=n_observed,
            unique=True,
        ).map(sorted)
    )
    observed = reference[indices] if indices else np.array([], dtype=np.int64)
    # Generate values matching observed length
    values = draw(
        np_st.arrays(
            dtype=np.float64,
            shape=len(observed),
            elements=st.floats(
                allow_nan=False,
                allow_infinity=False,
                min_value=-1e6,
                max_value=1e6,
            ),
        )
    )
    return reference, observed, values


@st.composite
def reference_subset_values_and_errors(draw, min_ref_size=1, max_ref_size=50):
    """Generate (reference, observed, values, errors) tuple."""
    reference, observed, values = draw(
        reference_and_valid_subset(min_ref_size, max_ref_size)
    )
    # Generate errors matching observed length
    errors = draw(
        np_st.arrays(
            dtype=np.float64,
            shape=len(observed),
            elements=st.floats(
                allow_nan=False,
                allow_infinity=False,
                min_value=0.0,  # Errors are non-negative
                max_value=1e3,
            ),
        )
    )
    return reference, observed, values, errors


class TestAlignToObservationValidation:
    """Test validation checks in DataSet.align_to_observation."""

    def test_raises_when_observation_is_none(self):
        """Test ValueError when observation is None."""
        dataset = DataSet(
            values=np.array([1.0, 2.0, 3.0]),
            observation=None,
        )
        cadences = np.array([1, 2, 3], dtype=np.int64)

        with pytest.raises(ValueError, match="no observation"):
            dataset.align_to_observation(cadences)

    def test_raises_when_values_is_none(self):
        """Test ValueError when values is None."""
        observation = Observation(
            cadence_reference=np.array([1, 2, 3, 4, 5], dtype=np.int64)
        )
        dataset = DataSet(
            values=None,
            observation=observation,
        )
        cadences = np.array([1, 2, 3], dtype=np.int64)

        with pytest.raises(ValueError, match="values array is None"):
            dataset.align_to_observation(cadences)


class TestAlignToObservationBasic:
    """Basic tests for DataSet.align_to_observation."""

    def test_align_values_only(self):
        """Test aligning dataset with values but no errors."""
        reference = np.array([1, 2, 3, 4, 5], dtype=np.int64)
        observation = Observation(cadence_reference=reference)

        # Dataset has values at cadences [2, 4]
        observed_cadences = np.array([2, 4], dtype=np.int64)
        values = np.array([100.0, 200.0])

        dataset = DataSet(values=values, observation=observation, errors=None)
        dataset.align_to_observation(observed_cadences)

        # Values should now be aligned to reference
        expected = np.array([np.nan, 100.0, np.nan, 200.0, np.nan])
        np.testing.assert_array_equal(
            np.isnan(dataset.values), np.isnan(expected)
        )
        assert dataset.values[1] == 100.0
        assert dataset.values[3] == 200.0
        assert dataset.errors is None

    def test_align_values_and_errors(self):
        """Test aligning dataset with both values and errors."""
        reference = np.array([1, 2, 3, 4, 5], dtype=np.int64)
        observation = Observation(cadence_reference=reference)

        # Dataset has values at cadences [2, 4]
        observed_cadences = np.array([2, 4], dtype=np.int64)
        values = np.array([100.0, 200.0])
        errors = np.array([0.1, 0.2])

        dataset = DataSet(
            values=values, observation=observation, errors=errors
        )
        dataset.align_to_observation(observed_cadences)

        # Both arrays should now be aligned
        assert len(dataset.values) == 5
        assert len(dataset.errors) == 5

        # Check values
        assert dataset.values[1] == 100.0
        assert dataset.values[3] == 200.0
        assert np.isnan(dataset.values[0])

        # Check errors
        assert dataset.errors[1] == 0.1
        assert dataset.errors[3] == 0.2
        assert np.isnan(dataset.errors[0])

    def test_custom_fill_value(self):
        """Test using custom fill_value."""
        reference = np.array([1, 2, 3], dtype=np.int64)
        observation = Observation(cadence_reference=reference)

        observed_cadences = np.array([2], dtype=np.int64)
        values = np.array([42.0])
        errors = np.array([0.5])

        dataset = DataSet(
            values=values, observation=observation, errors=errors
        )
        dataset.align_to_observation(observed_cadences, fill_value=-999.0)

        np.testing.assert_array_equal(dataset.values, [-999.0, 42.0, -999.0])
        np.testing.assert_array_equal(dataset.errors, [-999.0, 0.5, -999.0])

    def test_full_coverage(self):
        """Test when dataset cadences match reference exactly."""
        reference = np.array([1, 2, 3], dtype=np.int64)
        observation = Observation(cadence_reference=reference)

        values = np.array([10.0, 20.0, 30.0])
        errors = np.array([1.0, 2.0, 3.0])

        dataset = DataSet(
            values=values, observation=observation, errors=errors
        )
        dataset.align_to_observation(reference)

        np.testing.assert_array_equal(dataset.values, values)
        np.testing.assert_array_equal(dataset.errors, errors)


class TestAlignToObservationProperties:
    """Property-based tests for DataSet.align_to_observation."""

    @given(data=reference_and_valid_subset())
    def test_output_length_equals_reference(self, data):
        """Result length always equals reference length."""
        reference, observed, values = data
        observation = Observation(cadence_reference=reference)
        dataset = DataSet(values=values, observation=observation, errors=None)

        dataset.align_to_observation(observed)

        assert len(dataset.values) == len(reference)

    @given(data=reference_subset_values_and_errors())
    def test_values_and_errors_same_length(self, data):
        """Values and errors have same length after alignment."""
        reference, observed, values, errors = data
        observation = Observation(cadence_reference=reference)
        dataset = DataSet(
            values=values, observation=observation, errors=errors
        )

        dataset.align_to_observation(observed)

        assert len(dataset.values) == len(dataset.errors)
        assert len(dataset.values) == len(reference)

    @given(data=reference_and_valid_subset())
    def test_values_preserved_at_observed_positions(self, data):
        """Original values appear at correct positions in result."""
        reference, observed, values = data
        observation = Observation(cadence_reference=reference)
        original_values = values.copy()
        dataset = DataSet(values=values, observation=observation, errors=None)

        dataset.align_to_observation(observed)

        # Find where observed values should be in result
        indices = np.searchsorted(reference, observed)
        np.testing.assert_array_equal(dataset.values[indices], original_values)

    @given(data=reference_subset_values_and_errors())
    def test_errors_preserved_at_observed_positions(self, data):
        """Original errors appear at correct positions in result."""
        reference, observed, values, errors = data
        observation = Observation(cadence_reference=reference)
        original_errors = errors.copy()
        dataset = DataSet(
            values=values, observation=observation, errors=errors
        )

        dataset.align_to_observation(observed)

        indices = np.searchsorted(reference, observed)
        np.testing.assert_array_equal(dataset.errors[indices], original_errors)

    @given(data=reference_and_valid_subset(), fill=st.floats(allow_nan=True))
    def test_fill_value_at_gaps(self, data, fill):
        """Non-observed positions contain fill_value."""
        reference, observed, values = data
        assume(len(reference) > len(observed))  # Need gaps
        observation = Observation(cadence_reference=reference)
        dataset = DataSet(values=values, observation=observation, errors=None)

        dataset.align_to_observation(observed, fill_value=fill)

        # Create mask for gap positions
        observed_set = set(observed)
        gap_mask = np.array([r not in observed_set for r in reference])
        if np.isnan(fill):
            assert np.all(np.isnan(dataset.values[gap_mask]))
        else:
            assert np.all(dataset.values[gap_mask] == fill)


class TestAlignToObservationEdgeCases:
    """Edge case tests for DataSet.align_to_observation."""

    def test_empty_reference_empty_observed(self):
        """Empty reference with empty observed returns empty arrays."""
        observation = Observation(
            cadence_reference=np.array([], dtype=np.int64)
        )
        dataset = DataSet(
            values=np.array([], dtype=np.float64),
            observation=observation,
            errors=np.array([], dtype=np.float64),
        )

        dataset.align_to_observation(np.array([], dtype=np.int64))

        assert len(dataset.values) == 0
        assert len(dataset.errors) == 0

    def test_single_element(self):
        """Single element reference with matching observed."""
        observation = Observation(
            cadence_reference=np.array([42], dtype=np.int64)
        )
        dataset = DataSet(
            values=np.array([3.14]),
            observation=observation,
            errors=np.array([0.01]),
        )

        dataset.align_to_observation(np.array([42], dtype=np.int64))

        assert dataset.values[0] == 3.14
        assert dataset.errors[0] == 0.01

    def test_no_observed_returns_all_fill(self):
        """Empty observed array results in all fill_value."""
        reference = np.array([1, 2, 3, 4, 5], dtype=np.int64)
        observation = Observation(cadence_reference=reference)
        dataset = DataSet(
            values=np.array([], dtype=np.float64),
            observation=observation,
            errors=None,
        )

        fill = -999.0
        dataset.align_to_observation(
            np.array([], dtype=np.int64), fill_value=fill
        )

        assert np.all(dataset.values == fill)

    def test_mutates_in_place(self):
        """Verify that align_to_observation modifies dataset in place."""
        reference = np.array([1, 2, 3], dtype=np.int64)
        observation = Observation(cadence_reference=reference)
        original_values = np.array([10.0])

        dataset = DataSet(
            values=original_values, observation=observation, errors=None
        )
        original_id = id(dataset)

        dataset.align_to_observation(np.array([2], dtype=np.int64))

        # Same object, but values array is different
        assert id(dataset) == original_id
        assert len(dataset.values) == 3
