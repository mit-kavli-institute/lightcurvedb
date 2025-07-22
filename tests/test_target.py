"""Test Target model functionality."""

import uuid

import numpy as np
import pytest
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
from lightcurvedb.models.dataset import DataSet


class TestTargetBasics:
    """Test basic Target model functionality."""

    def test_create_target(self, v2_db: orm.Session):
        """Test creating a basic target."""
        # Create parent mission and catalog first
        mission = Mission(
            id=uuid.uuid4(),
            name="TESS",
            description="Transiting Exoplanet Survey Satellite",
            time_unit="day",
            time_epoch=2457000.0,
            time_epoch_scale="tdb",
            time_epoch_format="jd",
            time_format_name="btjd",
        )
        v2_db.add(mission)
        v2_db.flush()

        catalog = MissionCatalog(
            host_mission=mission,
            name="TIC",
            description="TESS Input Catalog",
        )
        v2_db.add(catalog)
        v2_db.flush()

        # Create target
        target = Target(
            catalog_id=catalog.id,
            name=12345678,
        )
        v2_db.add(target)
        v2_db.commit()

        # Verify creation
        assert target.id is not None
        assert target.name == 12345678
        assert target.catalog_id == catalog.id

    def test_create_target_with_relationship(self, v2_db: orm.Session):
        """Test creating target using relationship."""
        mission = Mission(
            id=uuid.uuid4(),
            name="Kepler",
            description="Kepler Space Telescope",
            time_unit="day",
            time_epoch=2454833.0,
            time_epoch_scale="utc",
            time_epoch_format="jd",
            time_format_name="kbjd",
        )
        catalog = MissionCatalog(
            host_mission=mission,
            name="KIC",
            description="Kepler Input Catalog",
        )
        v2_db.add_all([mission, catalog])
        v2_db.flush()

        # Create target using relationship
        target = Target(
            catalog=catalog,  # Using relationship instead of ID
            name=87654321,
        )
        v2_db.add(target)
        v2_db.commit()

        assert target.catalog == catalog
        assert target.catalog_id == catalog.id

    def test_target_large_name_value(self, v2_db: orm.Session):
        """Test target with large name value (BigInteger)."""
        mission = Mission(
            id=uuid.uuid4(),
            name="LARGE_NAME_MISSION",
            description="Mission for large name test",
            time_unit="day",
            time_epoch=0.0,
            time_epoch_scale="tdb",
            time_epoch_format="jd",
            time_format_name="test",
        )
        catalog = MissionCatalog(
            host_mission=mission,
            name="LARGE_CAT",
            description="Large catalog",
        )
        v2_db.add_all([mission, catalog])
        v2_db.flush()

        # Create target with very large name value
        large_name = 9223372036854775807  # Max signed 64-bit integer
        target = Target(
            catalog=catalog,
            name=large_name,
        )
        v2_db.add(target)
        v2_db.commit()

        assert target.name == large_name

    def test_target_minimal_fields(self, v2_db: orm.Session):
        """Test creating target with minimal required fields."""
        mission = Mission(
            id=uuid.uuid4(),
            name="MIN_MISSION",
            description="Minimal mission",
            time_unit="day",
            time_epoch=0.0,
            time_epoch_scale="tdb",
            time_epoch_format="jd",
            time_format_name="test",
        )
        catalog = MissionCatalog(
            host_mission=mission,
            name="MIN_CAT",
            description="Minimal catalog",
        )
        v2_db.add_all([mission, catalog])
        v2_db.flush()

        target = Target(
            catalog=catalog,
            name=1,  # Minimum valid name
        )
        v2_db.add(target)
        v2_db.commit()

        assert target.id is not None
        assert target.name == 1


class TestTargetRelationships:
    """Test Target relationships."""

    def test_target_catalog_relationship(self, v2_db: orm.Session):
        """Test Target <-> MissionCatalog relationship."""
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
        v2_db.add_all([mission, catalog])
        v2_db.flush()

        target = Target(
            catalog_id=catalog.id,
            name=12345678,
        )
        v2_db.add(target)
        v2_db.commit()

        # Test target -> catalog relationship
        assert target.catalog == catalog
        assert target.catalog.name == "TIC"

        # Test catalog -> targets relationship
        assert len(catalog.targets) == 1
        assert target in catalog.targets

    def test_target_quality_flag_arrays_relationship(self, v2_db: orm.Session):
        """Test Target <-> QualityFlagArray relationship."""
        import numpy as np

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
        instrument = Instrument(name="Test Camera", properties={})
        observation = Observation(
            instrument=instrument,
            cadence_reference=np.array([1, 2, 3], dtype=np.int64),
        )
        v2_db.add_all([mission, catalog, target, instrument, observation])
        v2_db.flush()

        # Create target-specific quality flags
        quality_flags = QualityFlagArray(
            observation=observation,
            target=target,
            quality_flags=np.array([0, 1, 2], dtype=np.int32),
        )
        v2_db.add(quality_flags)
        v2_db.commit()

        # Test target -> quality_flag_arrays relationship
        assert len(target.quality_flag_arrays) == 1
        assert quality_flags in target.quality_flag_arrays

        # Test quality_flag_array -> target relationship
        assert quality_flags.target == target

    def test_target_target_specific_times_relationship(
        self, v2_db: orm.Session
    ):
        """Test Target <-> TargetSpecificTime relationship."""
        import numpy as np

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
        instrument = Instrument(name="Test Camera", properties={})
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
            barycentric_julian_dates=np.array(
                [2458000.0, 2458001.0, 2458002.0], dtype=np.float64
            ),
        )
        v2_db.add(tst)
        v2_db.commit()

        # Test target -> target_specific_times relationship
        assert len(target.target_specific_times) == 1
        assert tst in target.target_specific_times

        # Test target_specific_time -> target relationship
        assert tst.target == target

    def test_target_multiple_relationships(self, v2_db: orm.Session):
        """Test target with multiple related objects."""
        import numpy as np

        mission = Mission(
            id=uuid.uuid4(),
            name="MULTI_REL_MISSION",
            description="Mission for multiple relationships test",
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
        target = Target(catalog=catalog, name=99999999)
        instrument = Instrument(name="Multi Instrument", properties={})
        v2_db.add_all([mission, catalog, target, instrument])
        v2_db.flush()

        # Create multiple observations
        observations = []
        for i in range(3):
            obs = Observation(
                instrument=instrument,
                cadence_reference=np.array([i, i + 1, i + 2], dtype=np.int64),
            )
            observations.append(obs)
        v2_db.add_all(observations)
        v2_db.flush()

        # Create quality flags for each observation
        for obs in observations:
            qf = QualityFlagArray(
                observation=obs,
                target=target,
                quality_flags=np.array([0, 1, 0], dtype=np.int32),
            )
            v2_db.add(qf)

        # Create target specific times for each observation
        for obs in observations:
            tst = TargetSpecificTime(
                target=target,
                observation=obs,
                barycentric_julian_dates=np.array(
                    [2458000.0, 2458001.0, 2458002.0], dtype=np.float64
                ),
            )
            v2_db.add(tst)

        v2_db.commit()

        # Verify all relationships
        assert len(target.quality_flag_arrays) == 3
        assert len(target.target_specific_times) == 3


class TestTargetConstraints:
    """Test Target database constraints."""

    def test_target_foreign_key_constraint(self, v2_db: orm.Session):
        """Test foreign key constraint for invalid catalog_id."""
        target = Target(
            catalog_id=999999,  # Non-existent catalog
            name=12345678,
        )
        v2_db.add(target)

        with pytest.raises(exc.IntegrityError):
            v2_db.commit()
        v2_db.rollback()

    def test_target_unique_name_per_catalog(self, v2_db: orm.Session):
        """Test unique constraint on (catalog_id, name)."""
        mission = Mission(
            id=uuid.uuid4(),
            name="UNIQUE_TEST_MISSION",
            description="Mission for unique test",
            time_unit="day",
            time_epoch=0.0,
            time_epoch_scale="tdb",
            time_epoch_format="jd",
            time_format_name="test",
        )
        catalog = MissionCatalog(
            host_mission=mission,
            name="UNIQUE_CAT",
            description="Unique catalog",
        )
        v2_db.add_all([mission, catalog])
        v2_db.flush()

        # Create first target
        target1 = Target(
            catalog=catalog,
            name=12345678,
        )
        v2_db.add(target1)
        v2_db.commit()

        # Try to create duplicate with same name in same catalog
        target2 = Target(
            catalog=catalog,
            name=12345678,  # Same name
        )
        v2_db.add(target2)

        with pytest.raises(exc.IntegrityError):
            v2_db.commit()
        v2_db.rollback()

    def test_target_same_name_different_catalogs(self, v2_db: orm.Session):
        """Test that same target name can exist in different catalogs."""
        mission = Mission(
            id=uuid.uuid4(),
            name="MULTI_CAT_MISSION",
            description="Mission with multiple catalogs",
            time_unit="day",
            time_epoch=0.0,
            time_epoch_scale="tdb",
            time_epoch_format="jd",
            time_format_name="test",
        )
        catalog1 = MissionCatalog(
            host_mission=mission,
            name="CATALOG1",
            description="First catalog",
        )
        catalog2 = MissionCatalog(
            host_mission=mission,
            name="CATALOG2",
            description="Second catalog",
        )
        v2_db.add_all([mission, catalog1, catalog2])
        v2_db.flush()

        # Create targets with same name in different catalogs
        target1 = Target(
            catalog=catalog1,
            name=12345678,  # Same name
        )
        target2 = Target(
            catalog=catalog2,
            name=12345678,  # Same name
        )
        v2_db.add_all([target1, target2])
        v2_db.commit()  # Should succeed

        assert target1.name == target2.name
        assert target1.catalog != target2.catalog

    def test_target_required_fields(self, v2_db: orm.Session):
        """Test required fields for Target."""
        mission = Mission(
            id=uuid.uuid4(),
            name="REQ_TEST_MISSION",
            description="Mission for required fields test",
            time_unit="day",
            time_epoch=0.0,
            time_epoch_scale="tdb",
            time_epoch_format="jd",
            time_format_name="test",
        )
        catalog = MissionCatalog(
            host_mission=mission,
            name="REQ_CAT",
            description="Required catalog",
        )
        v2_db.add_all([mission, catalog])
        v2_db.flush()

        # Try to create target without name
        target = Target(
            catalog=catalog,
            # name is missing
        )
        v2_db.add(target)

        with pytest.raises(exc.IntegrityError):
            v2_db.commit()
        v2_db.rollback()

    def test_target_cascade_behavior(self, v2_db: orm.Session):
        """Test cascade behavior when related objects are deleted."""

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
        target = Target(catalog=catalog, name=88888888)
        instrument = Instrument(name="Cascade Instrument", properties={})
        observation = Observation(
            instrument=instrument,
            cadence_reference=np.array([1, 2, 3], dtype=np.int64),
        )
        v2_db.add_all([mission, catalog, target, instrument, observation])
        v2_db.flush()

        # Create related objects
        quality_flags = QualityFlagArray(
            observation=observation,
            target=target,
            quality_flags=np.array([0, 1, 0], dtype=np.int32),
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

        target_id = target.id
        tst_id = tst.id

        # Delete target - related objects should handle this appropriately
        v2_db.execute(delete(Target).where(Target.id == target.id))
        v2_db.commit()

        # Verify target is deleted
        assert v2_db.query(Target).filter_by(id=target_id).first() is None

        assert v2_db.query(DataSet).filter_by(id=tst_id).first() is None


class TestTargetQueries:
    """Test various query patterns for Target model."""

    def test_query_targets_by_catalog(self, v2_db: orm.Session):
        """Test querying targets by catalog."""
        mission = Mission(
            id=uuid.uuid4(),
            name="QUERY_MISSION",
            description="Mission for query test",
            time_unit="day",
            time_epoch=0.0,
            time_epoch_scale="tdb",
            time_epoch_format="jd",
            time_format_name="test",
        )
        catalog1 = MissionCatalog(
            host_mission=mission,
            name="CATALOG1",
            description="First catalog",
        )
        catalog2 = MissionCatalog(
            host_mission=mission,
            name="CATALOG2",
            description="Second catalog",
        )
        v2_db.add_all([mission, catalog1, catalog2])
        v2_db.flush()

        # Add targets to catalog1
        targets1 = []
        for i in range(5):
            target = Target(catalog=catalog1, name=1000000 + i)
            targets1.append(target)
        v2_db.add_all(targets1)

        # Add targets to catalog2
        targets2 = []
        for i in range(3):
            target = Target(catalog=catalog2, name=2000000 + i)
            targets2.append(target)
        v2_db.add_all(targets2)
        v2_db.commit()

        # Query targets from catalog1
        catalog1_targets = (
            v2_db.query(Target).filter_by(catalog_id=catalog1.id).all()
        )
        assert len(catalog1_targets) == 5
        for target in targets1:
            assert target in catalog1_targets

        # Query targets from catalog2
        catalog2_targets = (
            v2_db.query(Target).filter_by(catalog_id=catalog2.id).all()
        )
        assert len(catalog2_targets) == 3

    def test_query_targets_by_name_range(self, v2_db: orm.Session):
        """Test querying targets by name range."""
        mission = Mission(
            id=uuid.uuid4(),
            name="RANGE_MISSION",
            description="Mission for range test",
            time_unit="day",
            time_epoch=0.0,
            time_epoch_scale="tdb",
            time_epoch_format="jd",
            time_format_name="test",
        )
        catalog = MissionCatalog(
            host_mission=mission,
            name="RANGE_CAT",
            description="Range catalog",
        )
        v2_db.add_all([mission, catalog])
        v2_db.flush()

        # Create targets with various names
        target_names = [100, 200, 300, 400, 500]
        targets = []
        for name in target_names:
            target = Target(catalog=catalog, name=name)
            targets.append(target)
        v2_db.add_all(targets)
        v2_db.commit()

        # Query targets in range
        targets_in_range = (
            v2_db.query(Target)
            .filter(Target.name >= 200)
            .filter(Target.name <= 400)
            .all()
        )
        assert len(targets_in_range) == 3
        assert all(200 <= t.name <= 400 for t in targets_in_range)

    def test_query_targets_with_observations(self, v2_db: orm.Session):
        """Test querying targets that have observations."""
        import numpy as np

        mission = Mission(
            id=uuid.uuid4(),
            name="OBS_MISSION",
            description="Mission for observation test",
            time_unit="day",
            time_epoch=0.0,
            time_epoch_scale="tdb",
            time_epoch_format="jd",
            time_format_name="test",
        )
        catalog = MissionCatalog(
            host_mission=mission,
            name="OBS_CAT",
            description="Observation catalog",
        )
        instrument = Instrument(name="Obs Instrument", properties={})
        v2_db.add_all([mission, catalog, instrument])
        v2_db.flush()

        # Create targets
        target_with_obs = Target(catalog=catalog, name=111111)
        target_without_obs = Target(catalog=catalog, name=222222)
        v2_db.add_all([target_with_obs, target_without_obs])
        v2_db.flush()

        # Create observation and TST for first target only
        observation = Observation(
            instrument=instrument,
            cadence_reference=np.array([1, 2, 3], dtype=np.int64),
        )
        v2_db.add(observation)
        v2_db.flush()

        tst = TargetSpecificTime(
            target=target_with_obs,
            observation=observation,
            barycentric_julian_dates=np.array(
                [2458000.0, 2458001.0, 2458002.0], dtype=np.float64
            ),
        )
        v2_db.add(tst)
        v2_db.commit()

        # Query targets with observations
        targets_with_observations = (
            v2_db.query(Target).join(TargetSpecificTime).distinct().all()
        )
        assert len(targets_with_observations) == 1
        assert target_with_obs in targets_with_observations
        assert target_without_obs not in targets_with_observations
