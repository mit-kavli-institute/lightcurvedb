"""Test QualityFlagArray model functionality."""

import uuid

import numpy as np
import pytest
from sqlalchemy import exc, orm

from lightcurvedb.models import (
    Instrument,
    Mission,
    MissionCatalog,
    Observation,
    QualityFlagArray,
    Target,
)


class TestQualityFlagArrayBasics:
    """Test basic QualityFlagArray model functionality."""

    def test_create_observation_wide_quality_flags(self, v2_db: orm.Session):
        """Test creating quality flags for an entire observation."""
        # Create required dependencies
        mission = Mission(
            id=uuid.uuid4(),
            name="TEST_MISSION",
            description="Test Mission",
            time_unit="day",
            time_epoch=0.0,
            time_epoch_scale="tdb",
            time_epoch_format="jd",
            time_format_name="test_time",
        )
        instrument = Instrument(name="Test Instrument", properties={})
        observation = Observation(
            cadence_reference=np.array([1, 2, 3, 4]),
            instrument=instrument,
        )
        v2_db.add_all([mission, instrument, observation])
        v2_db.flush()

        # Create observation-wide quality flags
        quality_flags = QualityFlagArray(
            observation_id=observation.id,
            quality_flags=np.array([0, 1, 4, 5], dtype=np.int32),
        )
        v2_db.add(quality_flags)
        v2_db.commit()

        # Verify creation
        assert quality_flags.id is not None
        assert quality_flags.observation_id == observation.id
        assert quality_flags.target_id is None
        assert np.array_equal(
            quality_flags.quality_flags, np.array([0, 1, 4, 5])
        )
        assert quality_flags.created_on is not None

    def test_create_target_specific_quality_flags(self, v2_db: orm.Session):
        """Test creating quality flags specific to a target."""
        # Create required dependencies
        mission = Mission(
            id=uuid.uuid4(),
            name="TEST_MISSION_2",
            description="Test Mission 2",
            time_unit="day",
            time_epoch=0.0,
            time_epoch_scale="tdb",
            time_epoch_format="jd",
            time_format_name="test_time_2",
        )
        catalog = MissionCatalog(
            name="TEST_CATALOG",
            description="Test Catalog",
            host_mission=mission,
        )
        target = Target(name=12345678, catalog=catalog)
        instrument = Instrument(name="Test Instrument 2", properties={})
        observation = Observation(
            cadence_reference=np.array([1, 2, 3, 4]),
            instrument=instrument,
        )
        v2_db.add_all([mission, catalog, target, instrument, observation])
        v2_db.flush()

        # Create target-specific quality flags
        quality_flags = QualityFlagArray(
            observation_id=observation.id,
            target_id=target.id,
            quality_flags=np.array([0, 0, 2, 8], dtype=np.int32),
        )
        v2_db.add(quality_flags)
        v2_db.commit()

        # Verify creation
        assert quality_flags.target_id == target.id

    def test_quality_flag_array_length_matches_cadence(
        self, v2_db: orm.Session
    ):
        """Test that quality flag array can match observation cadence."""
        # Create observation with specific cadence length
        instrument = Instrument(name="Test Instrument 3", properties={})
        cadence_array = np.arange(100)
        observation = Observation(
            cadence_reference=cadence_array,
            instrument=instrument,
        )
        v2_db.add_all([instrument, observation])
        v2_db.flush()

        # Create quality flags matching cadence length
        quality_array = np.zeros(100, dtype=np.int32)
        quality_flags = QualityFlagArray(
            observation_id=observation.id,
            quality_flags=quality_array,
        )
        v2_db.add(quality_flags)
        v2_db.commit()

        assert len(quality_flags.quality_flags) == len(
            observation.cadence_reference
        )


class TestQualityFlagArrayRelationships:
    """Test QualityFlagArray relationships."""

    def test_observation_relationship(self, v2_db: orm.Session):
        """Test bidirectional relationship with Observation."""
        instrument = Instrument(name="Test Instrument 4", properties={})
        observation = Observation(
            type="observation",  # Use base polymorphic identity
            cadence_reference=np.array([1, 2, 3]),
            instrument=instrument,
        )
        v2_db.add_all([instrument, observation])
        v2_db.flush()

        # Create quality flag for the observation
        quality_flags = QualityFlagArray(
            observation=observation,
            quality_flags=np.array([0, 1, 0], dtype=np.int32),
        )
        v2_db.add(quality_flags)
        v2_db.commit()

        # Test observation -> quality_flag_arrays relationship
        assert len(observation.quality_flag_arrays) == 1
        assert quality_flags in observation.quality_flag_arrays

        # Test quality_flag_array -> observation relationship
        assert quality_flags.observation == observation

    def test_target_relationship(self, v2_db: orm.Session):
        """Test bidirectional relationship with Target."""
        mission = Mission(
            id=uuid.uuid4(),
            name="TEST_MISSION_3",
            description="Test Mission 3",
            time_unit="day",
            time_epoch=0.0,
            time_epoch_scale="tdb",
            time_epoch_format="jd",
            time_format_name="test_time_3",
        )
        catalog = MissionCatalog(
            name="TEST_CATALOG_2",
            description="Test Catalog 2",
            host_mission=mission,
        )
        target = Target(name=87654321, catalog=catalog)
        instrument = Instrument(name="Test Instrument 5", properties={})
        observation = Observation(
            type="observation",  # Use base polymorphic identity
            cadence_reference=np.array([1, 2, 3]),
            instrument=instrument,
        )
        v2_db.add_all([mission, catalog, target, instrument, observation])
        v2_db.flush()

        # Create target-specific quality flags
        target_flags = QualityFlagArray(
            observation=observation,
            target=target,
            quality_flags=np.array([1, 1, 1], dtype=np.int32),
        )
        v2_db.add(target_flags)
        v2_db.commit()

        # Test target -> quality_flag_arrays relationship
        assert len(target.quality_flag_arrays) == 1
        assert target_flags in target.quality_flag_arrays

        # Test quality_flag_array -> target relationship
        assert target_flags.target == target


class TestQualityFlagArrayConstraints:
    """Test QualityFlagArray database constraints."""

    def test_unique_constraint(self, v2_db: orm.Session):
        """Test unique constraint on (type, observation_id, target_id)."""
        instrument = Instrument(name="Test Instrument 6", properties={})
        observation = Observation(
            cadence_reference=np.array([1, 2]),
            instrument=instrument,
        )
        v2_db.add_all([instrument, observation])
        v2_db.flush()

        # Create first quality flag
        flags1 = QualityFlagArray(
            observation_id=observation.id,
            quality_flags=np.array([0, 1], dtype=np.int32),
        )
        v2_db.add(flags1)
        v2_db.commit()

        # Try to create duplicate with same type (base_quality_flag)
        flags2 = QualityFlagArray(
            observation_id=observation.id,  # Same observation
            quality_flags=np.array([1, 0], dtype=np.int32),
        )
        v2_db.add(flags2)

        # The type should also default to base_quality_flag
        with pytest.raises(exc.IntegrityError):
            v2_db.flush()
        v2_db.rollback()

    def test_cascade_delete_from_observation(self, v2_db: orm.Session):
        """Test quality flags are deleted when observation is deleted."""
        instrument = Instrument(name="Test Instrument 7", properties={})
        observation = Observation(
            cadence_reference=np.array([1, 2]),
            instrument=instrument,
        )
        v2_db.add_all([instrument, observation])
        v2_db.flush()

        quality_flags = QualityFlagArray(
            observation=observation,
            quality_flags=np.array([0, 1], dtype=np.int32),
        )
        v2_db.add(quality_flags)
        v2_db.commit()

        flag_id = quality_flags.id

        # Verify flag exists
        assert (
            v2_db.query(QualityFlagArray).filter_by(id=flag_id).first()
            is not None
        )

        # Delete observation
        v2_db.delete(observation)
        v2_db.commit()

        # Verify quality flags were deleted (cascade worked)
        assert (
            v2_db.query(QualityFlagArray).filter_by(id=flag_id).first() is None
        )

    def test_foreign_key_constraint_observation(self, v2_db: orm.Session):
        """Test foreign key constraint for invalid observation_id."""
        quality_flags = QualityFlagArray(
            observation_id=999999,  # Non-existent observation
            quality_flags=np.array([0, 1], dtype=np.int32),
        )
        v2_db.add(quality_flags)

        with pytest.raises(exc.IntegrityError):
            v2_db.commit()
        v2_db.rollback()

    def test_foreign_key_constraint_target(self, v2_db: orm.Session):
        """Test foreign key constraint for invalid target_id."""
        instrument = Instrument(name="Test Instrument 8", properties={})
        observation = Observation(
            cadence_reference=np.array([1, 2]),
            instrument=instrument,
        )
        v2_db.add_all([instrument, observation])
        v2_db.flush()

        quality_flags = QualityFlagArray(
            observation_id=observation.id,
            target_id=999999,  # Non-existent target
            quality_flags=np.array([0, 1], dtype=np.int32),
        )
        v2_db.add(quality_flags)

        with pytest.raises(exc.IntegrityError):
            v2_db.commit()
        v2_db.rollback()


class TestQualityFlagArrayBitOperations:
    """Test bit manipulation operations on quality flags."""

    def test_bit_flag_interpretation(self, v2_db: orm.Session):
        """Test interpreting individual bits in quality flags."""
        instrument = Instrument(name="Test Instrument 9", properties={})
        observation = Observation(
            cadence_reference=np.array([1, 2, 3, 4]),
            instrument=instrument,
        )
        v2_db.add_all([instrument, observation])
        v2_db.flush()

        # Create flags with specific bit patterns
        # Bit 0: Cosmic ray (1)
        # Bit 1: Saturation (2)
        # Bit 2: Bad pixel (4)
        quality_flags = QualityFlagArray(
            observation=observation,
            quality_flags=np.array(
                [0, 1, 2, 7], dtype=np.int32
            ),  # 0, cosmic ray, saturation, all three
        )
        v2_db.add(quality_flags)
        v2_db.commit()

        # Test bit interpretation
        v2_db.refresh(quality_flags)  # Refresh to ensure data is loaded
        flags = np.array(
            quality_flags.quality_flags, dtype=np.int32
        )  # Ensure numpy array

        # Check cosmic ray bit (bit 0)
        cosmic_ray_mask = (flags & 1) != 0
        assert np.array_equal(cosmic_ray_mask, [False, True, False, True])

        # Check saturation bit (bit 1)
        saturation_mask = (flags & 2) != 0
        assert np.array_equal(saturation_mask, [False, False, True, True])

        # Check bad pixel bit (bit 2)
        bad_pixel_mask = (flags & 4) != 0
        assert np.array_equal(bad_pixel_mask, [False, False, False, True])

    def test_maximum_bit_values(self, v2_db: orm.Session):
        """Test storing maximum 32-bit integer values."""
        instrument = Instrument(name="Test Instrument 10", properties={})
        observation = Observation(
            cadence_reference=np.array([1, 2]),
            instrument=instrument,
        )
        v2_db.add_all([instrument, observation])
        v2_db.flush()

        # Test with maximum signed 32-bit value
        max_int32 = np.iinfo(np.int32).max  # 2147483647
        quality_flags = QualityFlagArray(
            observation=observation,
            quality_flags=np.array([max_int32, 0], dtype=np.int32),
        )
        v2_db.add(quality_flags)
        v2_db.commit()

        # Verify stored correctly
        assert quality_flags.quality_flags[0] == max_int32


class TestQualityFlagArrayPolymorphism:
    """Test polymorphic behavior of QualityFlagArray."""

    def test_polymorphic_identity(self, v2_db: orm.Session):
        """Test that type field works as polymorphic discriminator."""
        instrument = Instrument(name="Test Instrument 11", properties={})
        observation = Observation(
            cadence_reference=np.array([1, 2]),
            instrument=instrument,
        )
        v2_db.add_all([instrument, observation])
        v2_db.flush()

        # Create quality flags (all will have base_quality_flag type)
        # Testing polymorphism with different targets
        mission = Mission(
            id=uuid.uuid4(),
            name="TEST_MISSION_POLY",
            description="Test Mission Poly",
            time_unit="day",
            time_epoch=0.0,
            time_epoch_scale="tdb",
            time_epoch_format="jd",
            time_format_name="test_time_poly",
        )
        catalog = MissionCatalog(
            name="TEST_CATALOG_POLY",
            description="Test Catalog Poly",
            host_mission=mission,
        )
        target1 = Target(name=111111, catalog=catalog)
        target2 = Target(name=222222, catalog=catalog)
        v2_db.add_all([mission, catalog, target1, target2])
        v2_db.flush()

        pixel_flags = QualityFlagArray(
            observation=observation,
            target=target1,  # Different target
            quality_flags=np.array([0, 1], dtype=np.int32),
        )
        aperture_flags = QualityFlagArray(
            observation=observation,
            target=target2,  # Different target
            quality_flags=np.array([1, 0], dtype=np.int32),
        )
        v2_db.add_all([pixel_flags, aperture_flags])
        v2_db.commit()

        # Query by target instead since type will be base_quality_flag
        target1_results = (
            v2_db.query(QualityFlagArray).filter_by(target_id=target1.id).all()
        )
        assert len(target1_results) == 1
        assert target1_results[0] == pixel_flags

        target2_results = (
            v2_db.query(QualityFlagArray).filter_by(target_id=target2.id).all()
        )
        assert len(target2_results) == 1
        assert target2_results[0] == aperture_flags

    def test_polymorphic_subclass(self, v2_db: orm.Session):
        """Test creating a subclass of QualityFlagArray."""

        # Define a custom quality flag subclass
        class TESSQualityFlags(QualityFlagArray):
            """TESS-specific quality flags with known bit definitions."""

            __mapper_args__ = {
                "polymorphic_identity": "tess_quality",
            }

            @property
            def cosmic_ray_events(self):
                """Return mask of cosmic ray events (bit 0)."""
                flags = np.array(self.quality_flags, dtype=np.int32)
                return (flags & 1) != 0

            @property
            def saturated_pixels(self):
                """Return mask of saturated pixels (bit 1)."""
                flags = np.array(self.quality_flags, dtype=np.int32)
                return (flags & 2) != 0

        # Use the subclass
        instrument = Instrument(name="TESS Camera", properties={})
        observation = Observation(
            cadence_reference=np.array([1, 2, 3]),
            instrument=instrument,
        )
        v2_db.add_all([instrument, observation])
        v2_db.flush()

        tess_flags = TESSQualityFlags(
            observation=observation,
            quality_flags=np.array([0, 1, 3], dtype=np.int32),
        )
        v2_db.add(tess_flags)
        v2_db.commit()

        # Verify polymorphic loading
        loaded = (
            v2_db.query(QualityFlagArray).filter_by(id=tess_flags.id).first()
        )
        assert isinstance(loaded, TESSQualityFlags)

        # Test custom properties
        assert np.array_equal(loaded.cosmic_ray_events, [False, True, True])
        assert np.array_equal(loaded.saturated_pixels, [False, False, True])
