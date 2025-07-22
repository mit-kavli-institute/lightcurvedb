"""Test TargetSpecificTime model functionality."""

import uuid

import numpy as np
import pytest
from sqlalchemy import exc, orm

from lightcurvedb.models import (
    Instrument,
    Mission,
    MissionCatalog,
    Observation,
    Target,
    TargetSpecificTime,
)


class TestTargetSpecificTimeBasics:
    """Test basic TargetSpecificTime model functionality."""

    def test_create_target_specific_time(self, v2_db: orm.Session):
        """Test creating a basic target specific time."""
        # Create prerequisites
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
        catalog = MissionCatalog(
            host_mission=mission,
            name="TIC",
            description="TESS Input Catalog",
        )
        target = Target(catalog=catalog, name=12345678)
        instrument = Instrument(
            name="TESS Camera 1",
            properties={"type": "camera"},
        )
        observation = Observation(
            instrument=instrument,
            cadence_reference=np.array([1, 2, 3], dtype=np.int64),
        )
        v2_db.add_all([mission, catalog, target, instrument, observation])
        v2_db.flush()

        # Create target specific time
        bjd_array = np.array(
            [2458000.0, 2458001.0, 2458002.0], dtype=np.float64
        )
        tst = TargetSpecificTime(
            target_id=target.id,
            observation_id=observation.id,
            barycentric_julian_dates=bjd_array,
        )
        v2_db.add(tst)
        v2_db.commit()

        # Verify creation
        assert tst.id is not None
        assert tst.target_id == target.id
        assert tst.observation_id == observation.id
        assert np.array_equal(tst.barycentric_julian_dates, bjd_array)

    def test_create_tst_with_relationships(self, v2_db: orm.Session):
        """Test creating TST using relationships."""
        # Create prerequisites
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
        target = Target(catalog=catalog, name=87654321)
        instrument = Instrument(
            name="Kepler CCD",
            properties={"type": "ccd"},
        )
        observation = Observation(
            instrument=instrument,
            cadence_reference=np.array([10, 20, 30], dtype=np.int64),
        )
        v2_db.add_all([mission, catalog, target, instrument, observation])
        v2_db.flush()

        # Create TST using relationships
        bjd_array = np.array(
            [2454833.0, 2454834.0, 2454835.0], dtype=np.float64
        )
        tst = TargetSpecificTime(
            target=target,  # Using relationship
            observation=observation,  # Using relationship
            barycentric_julian_dates=bjd_array,
        )
        v2_db.add(tst)
        v2_db.commit()

        assert tst.target == target
        assert tst.observation == observation

    def test_tst_large_bjd_array(self, v2_db: orm.Session):
        """Test TST with large BJD array."""
        # Create prerequisites
        mission = Mission(
            id=uuid.uuid4(),
            name="LARGE_MISSION",
            description="Mission for large array test",
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
        target = Target(catalog=catalog, name=99999999)
        instrument = Instrument(
            name="Large Array Instrument",
            properties={},
        )
        # Create observation with matching cadence length
        large_cadence = np.arange(10000, dtype=np.int64)
        observation = Observation(
            instrument=instrument,
            cadence_reference=large_cadence,
        )
        v2_db.add_all([mission, catalog, target, instrument, observation])
        v2_db.flush()

        # Create TST with large BJD array
        large_bjd = np.linspace(2458000.0, 2458100.0, 10000, dtype=np.float64)
        tst = TargetSpecificTime(
            target=target,
            observation=observation,
            barycentric_julian_dates=large_bjd,
        )
        v2_db.add(tst)
        v2_db.commit()

        # Verify array is stored correctly
        assert len(tst.barycentric_julian_dates) == 10000
        assert tst.barycentric_julian_dates[0] == 2458000.0
        assert np.isclose(tst.barycentric_julian_dates[-1], 2458100.0)

    def test_tst_bjd_precision(self, v2_db: orm.Session):
        """Test BJD array maintains float64 precision."""
        # Create prerequisites
        mission = Mission(
            id=uuid.uuid4(),
            name="PRECISION_MISSION",
            description="Mission for precision test",
            time_unit="day",
            time_epoch=0.0,
            time_epoch_scale="tdb",
            time_epoch_format="jd",
            time_format_name="test",
        )
        catalog = MissionCatalog(
            host_mission=mission,
            name="PREC_CAT",
            description="Precision catalog",
        )
        target = Target(catalog=catalog, name=11111111)
        instrument = Instrument(
            name="Precision Instrument",
            properties={},
        )
        observation = Observation(
            instrument=instrument,
            cadence_reference=np.array([1, 2, 3], dtype=np.int64),
        )
        v2_db.add_all([mission, catalog, target, instrument, observation])
        v2_db.flush()

        # Create TST with high-precision BJD values
        # These values have many decimal places
        bjd_array = np.array(
            [2458000.123456789012, 2458001.987654321098, 2458002.555555555555],
            dtype=np.float64,
        )
        tst = TargetSpecificTime(
            target=target,
            observation=observation,
            barycentric_julian_dates=bjd_array,
        )
        v2_db.add(tst)
        v2_db.commit()

        # Refresh from database
        v2_db.refresh(tst)

        # Verify precision is maintained (within float64 limits)
        assert tst.barycentric_julian_dates.dtype == np.float64
        np.testing.assert_allclose(
            tst.barycentric_julian_dates, bjd_array, rtol=1e-15
        )


class TestTargetSpecificTimeRelationships:
    """Test TargetSpecificTime relationships."""

    def test_tst_target_relationship(self, v2_db: orm.Session):
        """Test TargetSpecificTime <-> Target relationship."""
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
        instrument = Instrument(name="Test Instrument", properties={})
        observation = Observation(
            instrument=instrument,
            cadence_reference=np.array([1, 2, 3], dtype=np.int64),
        )
        v2_db.add_all([mission, catalog, target, instrument, observation])
        v2_db.flush()

        # Create target specific time
        tst = TargetSpecificTime(
            target_id=target.id,
            observation_id=observation.id,
            barycentric_julian_dates=np.array(
                [2458000.0, 2458001.0, 2458002.0], dtype=np.float64
            ),
        )
        v2_db.add(tst)
        v2_db.flush()

        # Test tst -> target relationship
        assert tst.target == target

        # Test target -> target_specific_times relationship
        assert len(target.target_specific_times) == 1
        assert tst in target.target_specific_times

    def test_tst_observation_relationship(self, v2_db: orm.Session):
        """Test TargetSpecificTime <-> Observation relationship."""
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
        instrument = Instrument(name="Test Instrument", properties={})
        observation = Observation(
            instrument=instrument,
            cadence_reference=np.array([1, 2, 3], dtype=np.int64),
        )
        v2_db.add_all([mission, catalog, target, instrument, observation])
        v2_db.flush()

        # Create target specific time
        tst = TargetSpecificTime(
            target_id=target.id,
            observation_id=observation.id,
            barycentric_julian_dates=np.array(
                [2458000.0, 2458001.0, 2458002.0], dtype=np.float64
            ),
        )
        v2_db.add(tst)
        v2_db.flush()

        # Test tst -> observation relationship
        assert tst.observation == observation

        # Test observation -> target_specific_times relationship
        assert len(observation.target_specific_times) == 1
        assert tst in observation.target_specific_times

    def test_multiple_tst_per_target(self, v2_db: orm.Session):
        """Test multiple TSTs for same target across different observations."""
        # Create prerequisites
        mission = Mission(
            id=uuid.uuid4(),
            name="MULTI_TST_MISSION",
            description="Mission for multiple TST test",
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
        target = Target(catalog=catalog, name=88888888)
        instrument = Instrument(name="Multi TST Instrument", properties={})
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

        # Create TST for each observation
        tsts = []
        for i, obs in enumerate(observations):
            tst = TargetSpecificTime(
                target=target,
                observation=obs,
                barycentric_julian_dates=np.array(
                    [2458000.0 + i, 2458001.0 + i, 2458002.0 + i],
                    dtype=np.float64,
                ),
            )
            tsts.append(tst)
        v2_db.add_all(tsts)
        v2_db.commit()

        # Verify target has multiple TSTs
        assert len(target.target_specific_times) == 3
        for tst in tsts:
            assert tst in target.target_specific_times

        # Verify each observation has one TST
        for obs in observations:
            assert len(obs.target_specific_times) == 1

    def test_multiple_targets_per_observation(self, v2_db: orm.Session):
        """Test multiple targets observed in same observation."""
        # Create prerequisites
        mission = Mission(
            id=uuid.uuid4(),
            name="MULTI_TARGET_MISSION",
            description="Mission for multiple target test",
            time_unit="day",
            time_epoch=0.0,
            time_epoch_scale="tdb",
            time_epoch_format="jd",
            time_format_name="test",
        )
        catalog = MissionCatalog(
            host_mission=mission,
            name="MULTI_TARGET_CAT",
            description="Multi target catalog",
        )
        instrument = Instrument(name="Multi Target Instrument", properties={})
        observation = Observation(
            instrument=instrument,
            cadence_reference=np.array([1, 2, 3, 4, 5], dtype=np.int64),
        )
        v2_db.add_all([mission, catalog, instrument, observation])
        v2_db.flush()

        # Create multiple targets
        targets = []
        for i in range(4):
            target = Target(catalog=catalog, name=7000000 + i)
            targets.append(target)
        v2_db.add_all(targets)
        v2_db.flush()

        # Create TST for each target in the same observation
        tsts = []
        for target in targets:
            tst = TargetSpecificTime(
                target=target,
                observation=observation,
                barycentric_julian_dates=np.array(
                    [2458000.0, 2458001.0, 2458002.0, 2458003.0, 2458004.0],
                    dtype=np.float64,
                ),
            )
            tsts.append(tst)
        v2_db.add_all(tsts)
        v2_db.commit()

        # Verify observation has multiple TSTs
        assert len(observation.target_specific_times) == 4
        for tst in tsts:
            assert tst in observation.target_specific_times

        # Verify each target has one TST
        for target in targets:
            assert len(target.target_specific_times) == 1


class TestTargetSpecificTimeConstraints:
    """Test TargetSpecificTime database constraints."""

    def test_tst_foreign_key_constraint_target(self, v2_db: orm.Session):
        """Test foreign key constraint for invalid target_id."""
        # Create valid observation
        instrument = Instrument(name="FK Test Instrument", properties={})
        observation = Observation(
            instrument=instrument,
            cadence_reference=np.array([1, 2, 3], dtype=np.int64),
        )
        v2_db.add_all([instrument, observation])
        v2_db.flush()

        # Try to create TST with invalid target_id
        tst = TargetSpecificTime(
            target_id=999999,  # Non-existent target
            observation_id=observation.id,
            barycentric_julian_dates=np.array(
                [2458000.0, 2458001.0, 2458002.0], dtype=np.float64
            ),
        )
        v2_db.add(tst)

        with pytest.raises(exc.IntegrityError):
            v2_db.commit()
        v2_db.rollback()

    def test_tst_foreign_key_constraint_observation(self, v2_db: orm.Session):
        """Test foreign key constraint for invalid observation_id."""
        # Create valid target
        mission = Mission(
            id=uuid.uuid4(),
            name="FK_TEST_MISSION",
            description="FK test mission",
            time_unit="day",
            time_epoch=0.0,
            time_epoch_scale="tdb",
            time_epoch_format="jd",
            time_format_name="test",
        )
        catalog = MissionCatalog(
            host_mission=mission,
            name="FK_CAT",
            description="FK catalog",
        )
        target = Target(catalog=catalog, name=55555555)
        v2_db.add_all([mission, catalog, target])
        v2_db.flush()

        # Try to create TST with invalid observation_id
        tst = TargetSpecificTime(
            target_id=target.id,
            observation_id=999999,  # Non-existent observation
            barycentric_julian_dates=np.array(
                [2458000.0, 2458001.0, 2458002.0], dtype=np.float64
            ),
        )
        v2_db.add(tst)

        with pytest.raises(exc.IntegrityError):
            v2_db.commit()
        v2_db.rollback()

    def test_tst_unique_constraint(self, v2_db: orm.Session):
        """Test unique constraint on (target_id, observation_id)."""
        # Create prerequisites
        mission = Mission(
            id=uuid.uuid4(),
            name="UNIQUE_TEST_MISSION",
            description="Unique test mission",
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
        target = Target(catalog=catalog, name=44444444)
        instrument = Instrument(name="Unique Test Instrument", properties={})
        observation = Observation(
            instrument=instrument,
            cadence_reference=np.array([1, 2, 3], dtype=np.int64),
        )
        v2_db.add_all([mission, catalog, target, instrument, observation])
        v2_db.flush()

        # Create first TST
        tst1 = TargetSpecificTime(
            target=target,
            observation=observation,
            barycentric_julian_dates=np.array(
                [2458000.0, 2458001.0, 2458002.0], dtype=np.float64
            ),
        )
        v2_db.add(tst1)
        v2_db.commit()

        # Try to create duplicate TST for same target and observation
        tst2 = TargetSpecificTime(
            target=target,  # Same target
            observation=observation,  # Same observation
            barycentric_julian_dates=np.array(
                [2458003.0, 2458004.0, 2458005.0], dtype=np.float64
            ),  # Different data
        )
        v2_db.add(tst2)

        with pytest.raises(exc.IntegrityError):
            v2_db.commit()
        v2_db.rollback()

    def test_tst_required_fields(self, v2_db: orm.Session):
        """Test required fields for TargetSpecificTime."""
        # Create prerequisites
        mission = Mission(
            id=uuid.uuid4(),
            name="REQ_TEST_MISSION",
            description="Required test mission",
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
        target = Target(catalog=catalog, name=33333333)
        instrument = Instrument(name="Required Test Instrument", properties={})
        observation = Observation(
            instrument=instrument,
            cadence_reference=np.array([1, 2, 3], dtype=np.int64),
        )
        v2_db.add_all([mission, catalog, target, instrument, observation])
        v2_db.flush()

        # Try to create TST without barycentric_julian_dates
        tst = TargetSpecificTime(
            target=target,
            observation=observation,
            # barycentric_julian_dates is missing
        )
        v2_db.add(tst)

        with pytest.raises(exc.IntegrityError):
            v2_db.commit()
        v2_db.rollback()

    def test_tst_cascade_delete_from_target(self, v2_db: orm.Session):
        """Test TST is deleted when target is deleted."""
        # Create prerequisites
        mission = Mission(
            id=uuid.uuid4(),
            name="CASCADE_TARGET_MISSION",
            description="Cascade target test mission",
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
        target = Target(catalog=catalog, name=22222222)
        instrument = Instrument(name="Cascade Test Instrument", properties={})
        observation = Observation(
            instrument=instrument,
            cadence_reference=np.array([1, 2, 3], dtype=np.int64),
        )
        v2_db.add_all([mission, catalog, target, instrument, observation])
        v2_db.flush()

        # Create TST
        tst = TargetSpecificTime(
            target=target,
            observation=observation,
            barycentric_julian_dates=np.array(
                [2458000.0, 2458001.0, 2458002.0], dtype=np.float64
            ),
        )
        v2_db.add(tst)
        v2_db.commit()

        tst_id = tst.id
        target_id = target.id

        # Delete target
        v2_db.delete(target)
        v2_db.commit()

        # Verify target is deleted
        assert v2_db.query(Target).filter_by(id=target_id).first() is None

        # Verify TST is also deleted (cascade)
        assert (
            v2_db.query(TargetSpecificTime).filter_by(id=tst_id).first()
            is None
        )

    def test_tst_cascade_delete_from_observation(self, v2_db: orm.Session):
        """Test TST is deleted when observation is deleted."""
        # Create prerequisites
        mission = Mission(
            id=uuid.uuid4(),
            name="CASCADE_OBS_MISSION",
            description="Cascade observation test mission",
            time_unit="day",
            time_epoch=0.0,
            time_epoch_scale="tdb",
            time_epoch_format="jd",
            time_format_name="test",
        )
        catalog = MissionCatalog(
            host_mission=mission,
            name="CASCADE_OBS_CAT",
            description="Cascade obs catalog",
        )
        target = Target(catalog=catalog, name=11111111)
        instrument = Instrument(name="Cascade Obs Instrument", properties={})
        observation = Observation(
            instrument=instrument,
            cadence_reference=np.array([1, 2, 3], dtype=np.int64),
        )
        v2_db.add_all([mission, catalog, target, instrument, observation])
        v2_db.flush()

        # Create TST
        tst = TargetSpecificTime(
            target=target,
            observation=observation,
            barycentric_julian_dates=np.array(
                [2458000.0, 2458001.0, 2458002.0], dtype=np.float64
            ),
        )
        v2_db.add(tst)
        v2_db.commit()

        tst_id = tst.id
        obs_id = observation.id

        # Delete observation
        v2_db.delete(observation)
        v2_db.commit()

        # Verify observation is deleted
        assert v2_db.query(Observation).filter_by(id=obs_id).first() is None

        # Verify TST is also deleted (cascade)
        assert (
            v2_db.query(TargetSpecificTime).filter_by(id=tst_id).first()
            is None
        )


class TestTargetSpecificTimeQueries:
    """Test various query patterns for TargetSpecificTime model."""

    def test_query_tst_by_target(self, v2_db: orm.Session):
        """Test querying TSTs by target."""
        # Create prerequisites
        mission = Mission(
            id=uuid.uuid4(),
            name="QUERY_MISSION",
            description="Query test mission",
            time_unit="day",
            time_epoch=0.0,
            time_epoch_scale="tdb",
            time_epoch_format="jd",
            time_format_name="test",
        )
        catalog = MissionCatalog(
            host_mission=mission,
            name="QUERY_CAT",
            description="Query catalog",
        )
        target1 = Target(catalog=catalog, name=6000001)
        target2 = Target(catalog=catalog, name=6000002)
        instrument = Instrument(name="Query Instrument", properties={})
        v2_db.add_all([mission, catalog, target1, target2, instrument])
        v2_db.flush()

        # Create observations and TSTs
        for i in range(3):
            obs = Observation(
                instrument=instrument,
                cadence_reference=np.array([i, i + 1], dtype=np.int64),
            )
            v2_db.add(obs)
            v2_db.flush()

            # Create TST for target1
            tst1 = TargetSpecificTime(
                target=target1,
                observation=obs,
                barycentric_julian_dates=np.array(
                    [2458000.0 + i, 2458001.0 + i], dtype=np.float64
                ),
            )
            v2_db.add(tst1)

            # Create TST for target2 only for first observation
            if i == 0:
                tst2 = TargetSpecificTime(
                    target=target2,
                    observation=obs,
                    barycentric_julian_dates=np.array(
                        [2458000.0, 2458001.0], dtype=np.float64
                    ),
                )
                v2_db.add(tst2)

        v2_db.commit()

        # Query TSTs for target1
        target1_tsts = (
            v2_db.query(TargetSpecificTime)
            .filter_by(target_id=target1.id)
            .all()
        )
        assert len(target1_tsts) == 3

        # Query TSTs for target2
        target2_tsts = (
            v2_db.query(TargetSpecificTime)
            .filter_by(target_id=target2.id)
            .all()
        )
        assert len(target2_tsts) == 1

    def test_query_tst_by_bjd_range(self, v2_db: orm.Session):
        """Test querying TSTs by BJD range."""
        # Create prerequisites
        mission = Mission(
            id=uuid.uuid4(),
            name="BJD_RANGE_MISSION",
            description="BJD range test mission",
            time_unit="day",
            time_epoch=0.0,
            time_epoch_scale="tdb",
            time_epoch_format="jd",
            time_format_name="test",
        )
        catalog = MissionCatalog(
            host_mission=mission,
            name="BJD_CAT",
            description="BJD catalog",
        )
        target = Target(catalog=catalog, name=5000001)
        instrument = Instrument(name="BJD Instrument", properties={})
        v2_db.add_all([mission, catalog, target, instrument])
        v2_db.flush()

        # Create observations with different BJD ranges
        early_obs = Observation(
            instrument=instrument,
            cadence_reference=np.array([1, 2], dtype=np.int64),
        )
        late_obs = Observation(
            instrument=instrument,
            cadence_reference=np.array([3, 4], dtype=np.int64),
        )
        v2_db.add_all([early_obs, late_obs])
        v2_db.flush()

        # Create TSTs with different BJD ranges
        early_tst = TargetSpecificTime(
            target=target,
            observation=early_obs,
            barycentric_julian_dates=np.array(
                [2458000.0, 2458001.0], dtype=np.float64
            ),
        )
        late_tst = TargetSpecificTime(
            target=target,
            observation=late_obs,
            barycentric_julian_dates=np.array(
                [2458100.0, 2458101.0], dtype=np.float64
            ),
        )
        v2_db.add_all([early_tst, late_tst])
        v2_db.commit()

        # Query TSTs with BJD in specific range using PostgreSQL array funcs
        from sqlalchemy import func

        # TSTs containing BJDs > 2458050
        late_tsts = (
            v2_db.query(TargetSpecificTime)
            .filter(
                func.array_min(TargetSpecificTime.barycentric_julian_dates)
                > 2458050
            )
            .all()
        )
        assert len(late_tsts) == 1
        assert late_tst in late_tsts

        # TSTs with any BJD between 2458000 and 2458010
        range_tsts = (
            v2_db.query(TargetSpecificTime)
            .filter(
                func.array_min(TargetSpecificTime.barycentric_julian_dates)
                <= 2458010
            )
            .filter(
                func.array_max(TargetSpecificTime.barycentric_julian_dates)
                >= 2458000
            )
            .all()
        )
        assert len(range_tsts) == 1
        assert early_tst in range_tsts

    def test_query_tst_join_multiple_tables(self, v2_db: orm.Session):
        """Test complex queries joining multiple tables."""
        # Create prerequisites
        mission = Mission(
            id=uuid.uuid4(),
            name="TESS",
            description="TESS mission",
            time_unit="day",
            time_epoch=2457000.0,
            time_epoch_scale="tdb",
            time_epoch_format="jd",
            time_format_name="btjd",
        )
        catalog = MissionCatalog(
            host_mission=mission,
            name="TIC",
            description="TESS Input Catalog",
        )
        camera1 = Instrument(
            name="TESS Camera 1",
            properties={"type": "camera", "number": 1},
        )
        camera2 = Instrument(
            name="TESS Camera 2",
            properties={"type": "camera", "number": 2},
        )
        v2_db.add_all([mission, catalog, camera1, camera2])
        v2_db.flush()

        # Create targets
        targets = []
        for i in range(5):
            target = Target(catalog=catalog, name=4000000 + i)
            targets.append(target)
        v2_db.add_all(targets)
        v2_db.flush()

        # Create observations and TSTs
        for camera in [camera1, camera2]:
            for i in range(2):
                obs = Observation(
                    instrument=camera,
                    cadence_reference=np.array([i, i + 1], dtype=np.int64),
                )
                v2_db.add(obs)
                v2_db.flush()

                # Create TSTs for first 3 targets only
                for target in targets[:3]:
                    tst = TargetSpecificTime(
                        target=target,
                        observation=obs,
                        barycentric_julian_dates=np.array(
                            [2458000.0 + i, 2458001.0 + i], dtype=np.float64
                        ),
                    )
                    v2_db.add(tst)

        v2_db.commit()

        # Query TSTs for TESS targets observed by Camera 1
        tess_camera1_tsts = (
            v2_db.query(TargetSpecificTime)
            .join(Target)
            .join(MissionCatalog)
            .join(Mission)
            .join(
                Observation,
                TargetSpecificTime.observation_id == Observation.id,
            )
            .join(Instrument)
            .filter(Mission.name == "TESS")
            .filter(Instrument.name == "TESS Camera 1")
            .all()
        )
        assert len(tess_camera1_tsts) == 6  # 3 targets × 2 observations

        # Query unique targets with observations
        targets_with_obs = (
            v2_db.query(Target).join(TargetSpecificTime).distinct().all()
        )
        assert (
            len(targets_with_obs) == 3
        )  # Only first 3 targets have observations
