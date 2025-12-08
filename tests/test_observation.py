"""Test Observation model functionality."""

import uuid

import numpy as np
import pytest
from hypothesis import assume, given
from hypothesis import strategies as st
from hypothesis.extra import numpy as np_st
from sqlalchemy import delete, exc, orm

from lightcurvedb.models import (
    Instrument,
    Mission,
    MissionCatalog,
    Observation,
    QualityFlagArray,
    Target,
    TargetSpecificTime,
)


# Hypothesis strategies for align_to_reference tests
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
def reference_and_invalid_subset(draw):
    """Generate (reference, observed) where observed ⊄ reference."""
    reference = draw(monotonic_int64_array(min_size=1, max_size=50))
    # Create observed with at least one value not in reference
    max_val = reference.max() if len(reference) > 0 else 100
    invalid_val = draw(
        st.integers(min_value=int(max_val) + 1, max_value=int(max_val) + 1000)
    )
    observed = np.array([invalid_val], dtype=np.int64)
    return reference, observed


class TestObservationBasics:
    """Test basic Observation model functionality."""

    def test_create_observation(self, v2_db: orm.Session):
        """Test creating a basic observation."""
        # Create required instrument
        instrument = Instrument(
            name="TESS Camera 1",
            properties={"type": "camera"},
        )
        v2_db.add(instrument)
        v2_db.flush()

        # Create observation
        cadence_array = np.array([1, 2, 3, 4, 5], dtype=np.int64)
        observation = Observation(
            instrument=instrument,
            cadence_reference=cadence_array,
        )
        v2_db.add(observation)
        v2_db.commit()

        # Verify creation
        assert observation.id is not None
        assert observation.instrument_id == instrument.id
        assert np.array_equal(observation.cadence_reference, cadence_array)
        assert (
            observation.type == "observation"
        )  # Default polymorphic identity

    def test_create_observation_with_relationship(self, v2_db: orm.Session):
        """Test creating observation using relationship."""
        instrument = Instrument(
            name="Test Instrument",
            properties={"type": "ccd"},
        )
        v2_db.add(instrument)
        v2_db.flush()

        # Create observation using relationship
        observation = Observation(
            instrument=instrument,  # Using relationship instead of ID
            cadence_reference=np.array([10, 20, 30], dtype=np.int64),
        )
        v2_db.add(observation)
        v2_db.commit()

        assert observation.instrument == instrument
        assert observation.instrument_id == instrument.id

    def test_observation_large_cadence_array(self, v2_db: orm.Session):
        """Test observation with large cadence array."""
        instrument = Instrument(
            name="Large Array Instrument",
            properties={},
        )
        v2_db.add(instrument)
        v2_db.flush()

        # Create observation with large cadence array
        large_cadence = np.arange(10000, dtype=np.int64)
        observation = Observation(
            instrument=instrument,
            cadence_reference=large_cadence,
        )
        v2_db.add(observation)
        v2_db.commit()

        # Verify array is stored correctly
        assert len(observation.cadence_reference) == 10000
        assert observation.cadence_reference[0] == 0
        assert observation.cadence_reference[-1] == 9999

    def test_observation_empty_cadence_array(self, v2_db: orm.Session):
        """Test observation with empty cadence array."""
        instrument = Instrument(
            name="Empty Array Instrument",
            properties={},
        )
        v2_db.add(instrument)
        v2_db.flush()

        # Create observation with empty cadence array
        observation = Observation(
            instrument=instrument,
            cadence_reference=np.array([], dtype=np.int64),
        )
        v2_db.add(observation)
        v2_db.commit()

        assert len(observation.cadence_reference) == 0

    def test_observation_cadence_data_types(self, v2_db: orm.Session):
        """Test cadence array with proper data type."""
        instrument = Instrument(
            name="Type Test Instrument",
            properties={},
        )
        v2_db.add(instrument)
        v2_db.flush()

        # Test with int64 (correct type)
        cadence_int64 = np.array([1, 2, 3], dtype=np.int64)
        observation = Observation(
            instrument=instrument,
            cadence_reference=cadence_int64,
        )
        v2_db.add(observation)
        v2_db.commit()

        # With NumpyArrayType, arrays are automatically converted
        assert isinstance(observation.cadence_reference, np.ndarray)
        assert observation.cadence_reference.dtype == np.int64


class TestObservationRelationships:
    """Test Observation relationships."""

    def test_observation_instrument_relationship(self, v2_db: orm.Session):
        """Test Observation <-> Instrument relationship."""
        # Create instruments
        instrument1 = Instrument(
            id=uuid.uuid4(),
            name="Test CCD 1",
            properties={"type": "ccd"},
        )
        instrument2 = Instrument(
            id=uuid.uuid4(),
            name="Test CCD 2",
            properties={"type": "ccd"},
        )
        v2_db.add_all([instrument1, instrument2])
        v2_db.flush()

        # Create observation
        observation = Observation(
            instrument=instrument1,
            cadence_reference=np.array([1, 2, 3], dtype=np.int64),
        )
        v2_db.add(observation)
        v2_db.flush()

        # Test observation -> instrument relationship
        assert observation.instrument == instrument1

        # Test instrument -> observations relationship
        assert len(instrument1.observations) == 1
        assert observation in instrument1.observations
        assert len(instrument2.observations) == 0

    def test_observation_quality_flag_arrays_relationship(
        self, v2_db: orm.Session
    ):
        """Test Observation <-> QualityFlagArray relationship."""
        instrument = Instrument(
            name="Quality Test Instrument",
            properties={},
        )
        observation = Observation(
            instrument=instrument,
            cadence_reference=np.array([1, 2, 3], dtype=np.int64),
        )
        v2_db.add_all([instrument, observation])
        v2_db.flush()

        # Create quality flag arrays
        obs_wide_flags = QualityFlagArray(
            observation=observation,
            quality_flags=np.array([0, 1, 2], dtype=np.int32),
        )
        v2_db.add(obs_wide_flags)
        v2_db.commit()

        # Test observation -> quality_flag_arrays relationship
        assert len(observation.quality_flag_arrays) == 1
        assert obs_wide_flags in observation.quality_flag_arrays

        # Test quality_flag_array -> observation relationship
        assert obs_wide_flags.observation == observation

    def test_observation_target_specific_times_relationship(
        self, v2_db: orm.Session
    ):
        """Test Observation <-> TargetSpecificTime relationship."""
        # Create prerequisites
        mission = Mission(
            id=uuid.uuid4(),
            name="TESS",
            description="Test mission",
            time_unit="day",
            time_epoch=2457000.0,
            time_epoch_scale="tdb",
            time_epoch_format="jd",
            time_format_name="btjd",
        )
        catalog = MissionCatalog(
            host_mission=mission,
            name="TIC",
            description="Test catalog",
        )
        target = Target(catalog=catalog, name=12345678)
        instrument = Instrument(
            name="TST Test Instrument",
            properties={},
        )
        observation = Observation(
            instrument=instrument,
            cadence_reference=np.array([1, 2, 3], dtype=np.int64),
        )
        v2_db.add_all([mission, catalog, target, instrument, observation])
        v2_db.flush()

        # Create target specific time
        tst = TargetSpecificTime(
            target=target,
            observation=observation,
            barycentric_julian_dates=[2458000.0, 2458001.0, 2458002.0],
        )
        v2_db.add(tst)
        v2_db.commit()

        # Test observation -> target_specific_times relationship
        assert len(observation.target_specific_times) == 1
        assert tst in observation.target_specific_times

        # Test target_specific_time -> observation relationship
        assert tst.observation == observation

    def test_observation_multiple_relationships(self, v2_db: orm.Session):
        """Test observation with multiple related objects."""
        # Create prerequisites
        mission = Mission(
            id=uuid.uuid4(),
            name="MULTI_REL_MISSION",
            description="Mission for multiple relationships",
            time_unit="day",
            time_epoch=0.0,
            time_epoch_scale="tdb",
            time_epoch_format="jd",
            time_format_name="test",
        )
        catalog = MissionCatalog(
            host_mission=mission,
            name="MULTI_CAT",
            description="Multi catalog",
        )
        instrument = Instrument(
            name="Multi Rel Instrument",
            properties={},
        )
        observation = Observation(
            instrument=instrument,
            cadence_reference=np.array([1, 2, 3, 4, 5], dtype=np.int64),
        )
        v2_db.add_all([mission, catalog, instrument, observation])
        v2_db.flush()

        # Create multiple targets
        targets = []
        for i in range(3):
            target = Target(catalog=catalog, name=1000000 + i)
            targets.append(target)
        v2_db.add_all(targets)
        v2_db.flush()

        # Create quality flags (observation-wide and target-specific)
        obs_flags = QualityFlagArray(
            observation=observation,
            quality_flags=np.array([0, 0, 0, 0, 0], dtype=np.int32),
        )
        v2_db.add(obs_flags)

        for target in targets:
            target_flags = QualityFlagArray(
                observation=observation,
                target=target,
                quality_flags=np.array([1, 2, 3, 4, 5], dtype=np.int32),
            )
            v2_db.add(target_flags)

        # Create target specific times
        for target in targets:
            tst = TargetSpecificTime(
                target=target,
                observation=observation,
                barycentric_julian_dates=np.array(
                    [2458000.0, 2458001.0, 2458002.0, 2458003.0, 2458004.0],
                    dtype=np.float64,
                ),
            )
            v2_db.add(tst)

        v2_db.commit()

        # Verify all relationships
        assert (
            len(observation.quality_flag_arrays) == 4
        )  # 1 obs-wide + 3 target-specific
        assert len(observation.target_specific_times) == 3


class TestObservationConstraints:
    """Test Observation database constraints."""

    def test_observation_foreign_key_constraint(self, v2_db: orm.Session):
        """Test foreign key constraint for invalid instrument_id."""
        non_existent_uuid = uuid.uuid4()
        observation = Observation(
            instrument_id=non_existent_uuid,  # Non-existent instrument
            cadence_reference=np.array([1, 2, 3], dtype=np.int64),
        )
        v2_db.add(observation)

        with pytest.raises(exc.IntegrityError):
            v2_db.commit()
        v2_db.rollback()

    def test_observation_required_fields(self, v2_db: orm.Session):
        """Test required fields for Observation."""
        # Try to create observation without instrument
        observation = Observation(
            # instrument is missing
            cadence_reference=np.array([1, 2, 3], dtype=np.int64),
        )
        v2_db.add(observation)

        with pytest.raises(exc.IntegrityError):
            v2_db.commit()
        v2_db.rollback()

        # Try to create observation without cadence_reference
        instrument = Instrument(
            name="Required Test Instrument",
            properties={},
        )
        v2_db.add(instrument)
        v2_db.flush()

        observation2 = Observation(
            instrument=instrument,
            # cadence_reference is missing
        )
        v2_db.add(observation2)

        with pytest.raises(exc.IntegrityError):
            v2_db.commit()
        v2_db.rollback()

    def test_observation_cascade_delete_related(self, v2_db: orm.Session):
        """Test cascade deletion of related objects on observation delete."""
        # Create observation with related objects
        mission = Mission(
            id=uuid.uuid4(),
            name="CASCADE_TEST_MISSION",
            description="Mission for cascade test",
            time_unit="day",
            time_epoch=0.0,
            time_epoch_scale="tdb",
            time_epoch_format="jd",
            time_format_name="test",
        )
        catalog = MissionCatalog(
            host_mission=mission,
            name="CASCADE_CAT",
            description="Cascade catalog",
        )
        target = Target(catalog=catalog, name=99999999)
        instrument = Instrument(
            name="Cascade Test Instrument",
            properties={},
        )
        observation = Observation(
            instrument=instrument,
            cadence_reference=np.array([1, 2, 3], dtype=np.int64),
        )
        v2_db.add_all([mission, catalog, target, instrument, observation])
        v2_db.flush()

        # Create related objects
        quality_flags = QualityFlagArray(
            observation=observation,
            quality_flags=np.array([0, 1, 2], dtype=np.int32),
        )
        tst = TargetSpecificTime(
            target=target,
            observation=observation,
            barycentric_julian_dates=np.array(
                [2458000.0, 2458001.0, 2458002.0], dtype=np.float64
            ),
        )
        v2_db.add_all([quality_flags, tst])
        v2_db.commit()

        obs_id = observation.id
        qf_id = quality_flags.id
        tst_id = tst.id

        # Delete observation
        v2_db.execute(delete(Observation).where(Observation.id == obs_id))
        v2_db.commit()

        # Verify observation is deleted
        assert v2_db.query(Observation).filter_by(id=obs_id).first() is None

        # Verify related objects are deleted (cascade)
        assert (
            v2_db.query(QualityFlagArray).filter_by(id=qf_id).first() is None
        )
        assert (
            v2_db.query(TargetSpecificTime).filter_by(id=tst_id).first()
            is None
        )

    def test_observation_instrument_not_cascade_delete(
        self, v2_db: orm.Session
    ):
        """Test that deleting observation doesn't delete instrument."""
        instrument = Instrument(
            name="Persistent Instrument",
            properties={},
        )
        observation = Observation(
            instrument=instrument,
            cadence_reference=np.array([1, 2, 3], dtype=np.int64),
        )
        v2_db.add_all([instrument, observation])
        v2_db.commit()

        instrument_id = instrument.id
        obs_id = observation.id

        # Delete observation
        v2_db.delete(observation)
        v2_db.commit()

        # Verify observation is deleted
        assert v2_db.query(Observation).filter_by(id=obs_id).first() is None

        # Verify instrument still exists
        assert (
            v2_db.query(Instrument).filter_by(id=instrument_id).first()
            is not None
        )


class TestObservationPolymorphism:
    """Test polymorphic behavior of Observation model."""

    def test_observation_polymorphic_identity(self, v2_db: orm.Session):
        """Test default polymorphic identity."""
        instrument = Instrument(
            name="Poly Test Instrument",
            properties={},
        )
        observation = Observation(
            instrument=instrument,
            cadence_reference=np.array([1, 2, 3], dtype=np.int64),
        )
        v2_db.add_all([instrument, observation])
        v2_db.commit()

        # Verify default polymorphic identity
        assert observation.type == "observation"

    def test_observation_polymorphic_subclass(self, v2_db: orm.Session):
        """Test creating a subclass of Observation."""

        # Define a custom observation subclass
        class TESSObservation(Observation):
            """TESS-specific observation with additional properties."""

            __mapper_args__ = {
                "polymorphic_identity": "tess_observation",
            }

            @property
            def sector(self):
                """Extract sector from properties if available."""
                return self.instrument.properties.get("sector", None)

            @property
            def camera_number(self):
                """Extract camera number from properties."""
                return self.instrument.properties.get("camera", None)

        # Use the subclass
        instrument = Instrument(
            name="TESS Camera 1",
            properties={"type": "camera", "camera": 1, "sector": 42},
        )
        v2_db.add(instrument)
        v2_db.flush()

        tess_obs = TESSObservation(
            instrument=instrument,
            cadence_reference=np.array([1, 2, 3], dtype=np.int64),
        )
        v2_db.add(tess_obs)
        v2_db.commit()

        # Verify polymorphic loading
        loaded = v2_db.query(Observation).filter_by(id=tess_obs.id).first()
        assert isinstance(loaded, TESSObservation)
        assert loaded.type == "tess_observation"

        # Test custom properties
        assert loaded.sector == 42
        assert loaded.camera_number == 1

    def test_query_by_polymorphic_type(self, v2_db: orm.Session):
        """Test querying observations by polymorphic type."""

        class SpectroscopicObservation(Observation):
            """Spectroscopic observation subclass."""

            __mapper_args__ = {
                "polymorphic_identity": "spectroscopic_observation",
            }

        # Create different types of observations
        instrument = Instrument(
            name="Multi Type Instrument",
            properties={},
        )
        v2_db.add(instrument)
        v2_db.flush()

        # Base observation
        base_obs = Observation(
            instrument=instrument,
            cadence_reference=np.array([1, 2], dtype=np.int64),
        )

        # Spectroscopic observation
        spectro_obs = SpectroscopicObservation(
            instrument=instrument,
            cadence_reference=np.array([3, 4], dtype=np.int64),
        )

        v2_db.add_all([base_obs, spectro_obs])
        v2_db.commit()

        # Query all observations
        all_obs = v2_db.query(Observation).all()
        assert len(all_obs) == 2

        # Query only base observations
        base_only = (
            v2_db.query(Observation).filter_by(type="observation").all()
        )
        assert len(base_only) == 1
        assert base_obs in base_only

        # Query only spectroscopic observations
        spectro_only = (
            v2_db.query(Observation)
            .filter_by(type="spectroscopic_observation")
            .all()
        )
        assert len(spectro_only) == 1
        assert spectro_obs in spectro_only


class TestObservationQueries:
    """Test various query patterns for Observation model."""

    def test_query_observations_by_instrument(self, v2_db: orm.Session):
        """Test querying observations by instrument."""
        # Create instruments
        camera1 = Instrument(
            name="Camera 1",
            properties={"type": "camera"},
        )
        camera2 = Instrument(
            name="Camera 2",
            properties={"type": "camera"},
        )
        v2_db.add_all([camera1, camera2])
        v2_db.flush()

        # Create observations
        for i in range(5):
            obs = Observation(
                instrument=camera1,
                cadence_reference=np.array([i, i + 1, i + 2], dtype=np.int64),
            )
            v2_db.add(obs)

        for i in range(3):
            obs = Observation(
                instrument=camera2,
                cadence_reference=np.array([i, i + 1], dtype=np.int64),
            )
            v2_db.add(obs)

        v2_db.commit()

        # Query observations from camera1
        camera1_obs = (
            v2_db.query(Observation).filter_by(instrument_id=camera1.id).all()
        )
        assert len(camera1_obs) == 5

        # Query observations from camera2
        camera2_obs = (
            v2_db.query(Observation).filter_by(instrument_id=camera2.id).all()
        )
        assert len(camera2_obs) == 3

    def test_query_observations_by_cadence_length(self, v2_db: orm.Session):
        """Test querying observations by cadence array length."""
        instrument = Instrument(
            name="Length Test Instrument",
            properties={},
        )
        v2_db.add(instrument)
        v2_db.flush()

        # Create observations with different cadence lengths
        short_obs = Observation(
            instrument=instrument,
            cadence_reference=np.array([1, 2], dtype=np.int64),
        )
        medium_obs = Observation(
            instrument=instrument,
            cadence_reference=np.array([1, 2, 3, 4, 5], dtype=np.int64),
        )
        long_obs = Observation(
            instrument=instrument,
            cadence_reference=np.arange(100, dtype=np.int64),
        )
        v2_db.add_all([short_obs, medium_obs, long_obs])
        v2_db.commit()

        # Query by array length using PostgreSQL array functions
        from sqlalchemy import func

        # Observations with cadence length > 10
        long_cadence_obs = (
            v2_db.query(Observation)
            .filter(func.array_length(Observation.cadence_reference, 1) > 10)
            .all()
        )
        assert len(long_cadence_obs) == 1
        assert long_obs in long_cadence_obs

        # Observations with cadence length between 2 and 5
        medium_cadence_obs = (
            v2_db.query(Observation)
            .filter(
                func.array_length(Observation.cadence_reference, 1).between(
                    2, 5
                )
            )
            .all()
        )
        assert len(medium_cadence_obs) == 2
        assert short_obs in medium_cadence_obs
        assert medium_obs in medium_cadence_obs

    def test_query_observations_with_quality_flags(self, v2_db: orm.Session):
        """Test querying observations that have quality flags."""
        instrument = Instrument(
            name="QF Query Instrument",
            properties={},
        )
        v2_db.add(instrument)
        v2_db.flush()

        # Create observations
        obs_with_flags = Observation(
            instrument=instrument,
            cadence_reference=np.array([1, 2, 3], dtype=np.int64),
        )
        obs_without_flags = Observation(
            instrument=instrument,
            cadence_reference=np.array([4, 5, 6], dtype=np.int64),
        )
        v2_db.add_all([obs_with_flags, obs_without_flags])
        v2_db.flush()

        # Add quality flags to first observation only
        quality_flags = QualityFlagArray(
            observation=obs_with_flags,
            quality_flags=np.array([0, 1, 2], dtype=np.int32),
        )
        v2_db.add(quality_flags)
        v2_db.commit()

        # Query observations with quality flags
        obs_with_qf = (
            v2_db.query(Observation).join(QualityFlagArray).distinct().all()
        )
        assert len(obs_with_qf) == 1
        assert obs_with_flags in obs_with_qf
        assert obs_without_flags not in obs_with_qf


class TestAlignToReferenceProperties:
    """Property-based tests for Observation.align_to_reference."""

    @given(data=reference_and_valid_subset())
    def test_output_length_equals_reference(self, data):
        """Result length always equals reference length."""
        reference, observed, values = data
        obs = Observation(cadence_reference=reference)
        result = obs.align_to_reference(observed, values)
        assert len(result) == len(reference)

    @given(data=reference_and_valid_subset())
    def test_values_preserved_at_observed_positions(self, data):
        """Values appear at correct positions in result."""
        reference, observed, values = data
        obs = Observation(cadence_reference=reference)
        result = obs.align_to_reference(observed, values)
        # Find where observed values should be in result
        indices = np.searchsorted(reference, observed)
        np.testing.assert_array_equal(result[indices], values)

    @given(data=reference_and_valid_subset(), fill=st.floats(allow_nan=True))
    def test_fill_value_at_gaps(self, data, fill):
        """Non-observed positions contain fill_value."""
        reference, observed, values = data
        assume(len(reference) > len(observed))  # Need gaps
        obs = Observation(cadence_reference=reference)
        result = obs.align_to_reference(observed, values, fill_value=fill)
        # Create mask for gap positions
        observed_set = set(observed)
        gap_mask = np.array([r not in observed_set for r in reference])
        if np.isnan(fill):
            assert np.all(np.isnan(result[gap_mask]))
        else:
            assert np.all(result[gap_mask] == fill)

    @given(data=reference_and_valid_subset())
    def test_dtype_correctness(self, data):
        """Result dtype matches np.result_type(values, fill_value)."""
        reference, observed, values = data
        fill_value = np.nan
        obs = Observation(cadence_reference=reference)
        result = obs.align_to_reference(
            observed, values, fill_value=fill_value
        )
        expected_dtype = np.result_type(values, fill_value)
        assert result.dtype == expected_dtype

    @given(data=reference_and_invalid_subset())
    def test_verify_subset_raises_on_invalid(self, data):
        """verify_subset=True raises ValueError for invalid observed."""
        reference, observed = data
        values = np.ones(len(observed), dtype=np.float64)
        obs = Observation(cadence_reference=reference)
        with pytest.raises(
            ValueError, match="observed contains values not in reference"
        ):
            obs.align_to_reference(observed, values, verify_subset=True)

    @given(reference=monotonic_int64_array(min_size=1, max_size=50))
    def test_empty_observed_returns_all_fill(self, reference):
        """Empty observed array returns all fill_value."""
        obs = Observation(cadence_reference=reference)
        fill = -999.0
        result = obs.align_to_reference(
            np.array([], dtype=np.int64),
            np.array([], dtype=np.float64),
            fill_value=fill,
        )
        assert np.all(result == fill)

    @given(reference=monotonic_int64_array(min_size=1, max_size=50))
    def test_full_coverage_equals_values(self, reference):
        """When observed == reference, result equals values."""
        obs = Observation(cadence_reference=reference)
        values = np.arange(len(reference), dtype=np.float64)
        result = obs.align_to_reference(reference, values)
        np.testing.assert_array_equal(result, values)

    @given(data=reference_and_valid_subset())
    def test_roundtrip_recovery(self, data):
        """Can recover original values at observed positions."""
        reference, observed, values = data
        obs = Observation(cadence_reference=reference)
        result = obs.align_to_reference(observed, values, fill_value=np.nan)
        indices = np.searchsorted(reference, observed)
        recovered = result[indices]
        np.testing.assert_array_equal(recovered, values)


class TestAlignToReferenceEdgeCases:
    """Edge case tests for align_to_reference."""

    def test_empty_reference_empty_observed(self):
        """Empty reference with empty observed returns empty array."""
        obs = Observation(cadence_reference=np.array([], dtype=np.int64))
        result = obs.align_to_reference(
            np.array([], dtype=np.int64),
            np.array([], dtype=np.float64),
        )
        assert len(result) == 0

    def test_single_element_match(self):
        """Single element reference with matching observed."""
        obs = Observation(cadence_reference=np.array([42], dtype=np.int64))
        result = obs.align_to_reference(
            np.array([42], dtype=np.int64),
            np.array([3.14], dtype=np.float64),
        )
        assert result[0] == 3.14

    def test_integer_fill_value(self):
        """Test with integer fill_value."""
        obs = Observation(
            cadence_reference=np.array([1, 2, 3], dtype=np.int64)
        )
        result = obs.align_to_reference(
            np.array([2], dtype=np.int64),
            np.array([100], dtype=np.int64),
            fill_value=0,
        )
        np.testing.assert_array_equal(result, [0, 100, 0])

    def test_verify_subset_false_no_error(self):
        """verify_subset=False doesn't raise even with invalid observed."""
        obs = Observation(
            cadence_reference=np.array([1, 2, 3], dtype=np.int64)
        )
        # This won't raise, but result may be incorrect
        result = obs.align_to_reference(
            np.array([99], dtype=np.int64),
            np.array([1.0], dtype=np.float64),
            verify_subset=False,
        )
        assert len(result) == 3  # No error, just potentially wrong result
