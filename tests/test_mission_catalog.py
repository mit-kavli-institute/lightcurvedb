"""Test MissionCatalog model functionality."""

import uuid

import pytest
from sqlalchemy import delete, exc, orm

from lightcurvedb.models import Mission, MissionCatalog, Target


class TestMissionCatalogBasics:
    """Test basic MissionCatalog model functionality."""

    def test_create_mission_catalog(self, v2_db: orm.Session):
        """Test creating a basic mission catalog."""
        # Create parent mission first
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

        # Create catalog
        catalog = MissionCatalog(
            host_mission_id=mission.id,
            name="TIC",
            description="TESS Input Catalog",
        )
        v2_db.add(catalog)
        v2_db.commit()

        # Verify creation
        assert catalog.id is not None
        assert catalog.name == "TIC"
        assert catalog.description == "TESS Input Catalog"
        assert catalog.host_mission_id == mission.id

    def test_create_catalog_with_relationship(self, v2_db: orm.Session):
        """Test creating catalog using relationship."""
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
        v2_db.add(mission)
        v2_db.flush()

        # Create catalog using relationship
        catalog = MissionCatalog(
            host_mission=mission,  # Using relationship instead of ID
            name="KIC",
            description="Kepler Input Catalog",
        )
        v2_db.add(catalog)
        v2_db.commit()

        assert catalog.host_mission == mission
        assert catalog.host_mission_id == mission.id

    def test_catalog_minimal_fields(self, v2_db: orm.Session):
        """Test creating catalog with minimal required fields."""
        mission = Mission(
            id=uuid.uuid4(),
            name="MINIMAL_MISSION",
            description="Mission for minimal catalog test",
            time_unit="day",
            time_epoch=0.0,
            time_epoch_scale="tdb",
            time_epoch_format="jd",
            time_format_name="test",
        )
        v2_db.add(mission)
        v2_db.flush()

        catalog = MissionCatalog(
            host_mission=mission,
            name="MIN_CAT",
            description="Minimal catalog",
        )
        v2_db.add(catalog)
        v2_db.commit()

        assert catalog.id is not None
        assert catalog.name == "MIN_CAT"

    def test_catalog_long_description(self, v2_db: orm.Session):
        """Test catalog with very long description."""
        mission = Mission(
            id=uuid.uuid4(),
            name="LONG_DESC_MISSION",
            description="Mission for long description test",
            time_unit="day",
            time_epoch=0.0,
            time_epoch_scale="tdb",
            time_epoch_format="jd",
            time_format_name="test",
        )
        v2_db.add(mission)
        v2_db.flush()

        long_description = "B" * 1000  # 1000 character description
        catalog = MissionCatalog(
            host_mission=mission,
            name="LONG_DESC_CAT",
            description=long_description,
        )
        v2_db.add(catalog)
        v2_db.commit()

        assert len(catalog.description) == 1000
        assert catalog.description == long_description


class TestMissionCatalogRelationships:
    """Test MissionCatalog relationships."""

    def test_catalog_mission_relationship(self, v2_db: orm.Session):
        """Test MissionCatalog <-> Mission relationship."""
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
        v2_db.add(mission)
        v2_db.flush()

        catalog = MissionCatalog(
            host_mission_id=mission.id,
            name="TIC",
            description="Test catalog",
        )
        v2_db.add(catalog)
        v2_db.commit()

        # Test catalog -> mission relationship
        assert catalog.host_mission == mission
        assert catalog.host_mission.name == "TESS"

        # Test mission -> catalogs relationship
        assert len(mission.catalogs) == 1
        assert catalog in mission.catalogs

    def test_catalog_targets_relationship(self, v2_db: orm.Session):
        """Test MissionCatalog <-> Target relationship."""
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
        v2_db.add(mission)
        v2_db.flush()

        catalog = MissionCatalog(
            host_mission_id=mission.id,
            name="TIC",
            description="Test catalog",
        )
        v2_db.add(catalog)
        v2_db.flush()

        # Create targets
        target1 = Target(catalog_id=catalog.id, name=12345678)
        target2 = Target(catalog_id=catalog.id, name=87654321)
        v2_db.add_all([target1, target2])
        v2_db.flush()

        # Test catalog -> targets relationship
        assert len(catalog.targets) == 2
        assert target1 in catalog.targets
        assert target2 in catalog.targets

        # Test targets -> catalog relationship
        assert target1.catalog == catalog
        assert target2.catalog == catalog

    def test_catalog_many_targets(self, v2_db: orm.Session):
        """Test catalog with many targets."""
        mission = Mission(
            id=uuid.uuid4(),
            name="LARGE_MISSION",
            description="Mission with large catalog",
            time_unit="day",
            time_epoch=0.0,
            time_epoch_scale="tdb",
            time_epoch_format="jd",
            time_format_name="test",
        )
        v2_db.add(mission)
        v2_db.flush()

        catalog = MissionCatalog(
            host_mission=mission,
            name="LARGE_CAT",
            description="Large catalog",
        )
        v2_db.add(catalog)
        v2_db.flush()

        # Create many targets
        targets = []
        for i in range(10):
            target = Target(
                catalog=catalog,
                name=1000000 + i,  # Sequential target IDs
            )
            targets.append(target)
        v2_db.add_all(targets)
        v2_db.commit()

        # Verify all targets are associated
        assert len(catalog.targets) == 10
        for target in targets:
            assert target in catalog.targets
            assert target.catalog == catalog


class TestMissionCatalogConstraints:
    """Test MissionCatalog database constraints."""

    def test_catalog_foreign_key_constraint(self, v2_db: orm.Session):
        """Test foreign key constraint for invalid host_mission_id."""
        non_existent_uuid = uuid.uuid4()
        catalog = MissionCatalog(
            host_mission_id=non_existent_uuid,  # Non-existent mission
            name="ORPHAN_CATALOG",
            description="Catalog without mission",
        )
        v2_db.add(catalog)

        with pytest.raises(exc.IntegrityError):
            v2_db.commit()
        v2_db.rollback()

    def test_catalog_unique_name_per_mission(self, v2_db: orm.Session):
        """Test unique constraint on (host_mission_id, name)."""
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
        v2_db.add(mission)
        v2_db.flush()

        # Create first catalog
        catalog1 = MissionCatalog(
            host_mission=mission,
            name="UNIQUE_CATALOG",
            description="First catalog",
        )
        v2_db.add(catalog1)
        v2_db.commit()

        # Try to create duplicate with same name for same mission
        catalog2 = MissionCatalog(
            host_mission=mission,
            name="UNIQUE_CATALOG",  # Same name
            description="Second catalog",
        )
        v2_db.add(catalog2)

        with pytest.raises(exc.IntegrityError):
            v2_db.commit()
        v2_db.rollback()

    def test_catalog_same_name_different_missions(self, v2_db: orm.Session):
        """Test that same catalog name can exist for different missions."""
        # Create two missions
        mission1 = Mission(
            name="MISSION1",
            description="First mission",
            time_unit="day",
            time_epoch=0.0,
            time_epoch_scale="tdb",
            time_epoch_format="jd",
            time_format_name="test1",
        )
        mission2 = Mission(
            name="MISSION2",
            description="Second mission",
            time_unit="day",
            time_epoch=0.0,
            time_epoch_scale="tdb",
            time_epoch_format="jd",
            time_format_name="test2",
        )
        v2_db.add_all([mission1, mission2])
        v2_db.flush()

        # Create catalogs with same name for different missions
        catalog1 = MissionCatalog(
            host_mission=mission1,
            name="INPUT_CATALOG",  # Same name
            description="Input catalog for mission 1",
        )
        catalog2 = MissionCatalog(
            host_mission=mission2,
            name="INPUT_CATALOG",  # Same name
            description="Input catalog for mission 2",
        )
        v2_db.add_all([catalog1, catalog2])
        v2_db.commit()  # Should succeed

        assert catalog1.name == catalog2.name
        assert catalog1.host_mission != catalog2.host_mission

    def test_catalog_required_fields(self, v2_db: orm.Session):
        """Test required fields for MissionCatalog."""
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
        v2_db.add(mission)
        v2_db.flush()

        # Try to create catalog without name
        catalog = MissionCatalog(
            host_mission=mission,
            # name is missing
            description="Invalid catalog",
        )
        v2_db.add(catalog)

        with pytest.raises(exc.IntegrityError):
            v2_db.commit()
        v2_db.rollback()

    def test_catalog_cascade_delete_targets(self, v2_db: orm.Session):
        """Test cascade deletion of targets when catalog is deleted."""
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
        v2_db.add(mission)
        v2_db.flush()

        catalog = MissionCatalog(
            host_mission=mission,
            name="CASCADE_TEST_CATALOG",
            description="Catalog for cascade test",
        )
        v2_db.add(catalog)
        v2_db.flush()

        target = Target(
            catalog=catalog,
            name=99999999,
        )
        v2_db.add(target)
        v2_db.commit()

        catalog_id = catalog.id
        target_id = target.id

        # Delete catalog
        v2_db.execute(
            delete(MissionCatalog).where(MissionCatalog.id == catalog_id)
        )
        v2_db.commit()

        # Verify catalog is deleted
        assert (
            v2_db.query(MissionCatalog).filter_by(id=catalog_id).first()
            is None
        )

        # Verify target is also deleted (cascade)
        assert v2_db.query(Target).filter_by(id=target_id).first() is None


class TestMissionCatalogQueries:
    """Test various query patterns for MissionCatalog model."""

    def test_query_catalogs_by_mission(self, v2_db: orm.Session):
        """Test querying catalogs by mission."""
        # Create missions with catalogs
        mission1 = Mission(
            id=uuid.uuid4(),
            name="TESS",
            description="TESS mission",
            time_unit="day",
            time_epoch=2457000.0,
            time_epoch_scale="tdb",
            time_epoch_format="jd",
            time_format_name="btjd",
        )
        mission2 = Mission(
            id=uuid.uuid4(),
            name="Kepler",
            description="Kepler mission",
            time_unit="day",
            time_epoch=2454833.0,
            time_epoch_scale="utc",
            time_epoch_format="jd",
            time_format_name="kbjd",
        )
        v2_db.add_all([mission1, mission2])
        v2_db.flush()

        # Add catalogs to mission1
        tess_catalogs = []
        for name in ["TIC", "CTL", "TOI"]:
            catalog = MissionCatalog(
                host_mission=mission1,
                name=name,
                description=f"{name} catalog",
            )
            tess_catalogs.append(catalog)
        v2_db.add_all(tess_catalogs)

        # Add catalogs to mission2
        kepler_catalog = MissionCatalog(
            host_mission=mission2,
            name="KIC",
            description="Kepler Input Catalog",
        )
        v2_db.add(kepler_catalog)
        v2_db.commit()

        # Query all TESS catalogs
        tess_results = (
            v2_db.query(MissionCatalog)
            .join(Mission)
            .filter(Mission.name == "TESS")
            .all()
        )
        assert len(tess_results) == 3
        for catalog in tess_catalogs:
            assert catalog in tess_results

        # Query by catalog name pattern
        input_catalogs = (
            v2_db.query(MissionCatalog)
            .filter(MissionCatalog.name.like("%IC"))
            .all()
        )
        assert len(input_catalogs) == 2  # TIC and KIC

    def test_query_catalogs_with_target_count(self, v2_db: orm.Session):
        """Test querying catalogs with target counts."""
        mission = Mission(
            id=uuid.uuid4(),
            name="TARGET_COUNT_MISSION",
            description="Mission for target count test",
            time_unit="day",
            time_epoch=0.0,
            time_epoch_scale="tdb",
            time_epoch_format="jd",
            time_format_name="test",
        )
        v2_db.add(mission)
        v2_db.flush()

        # Create catalogs with different numbers of targets
        catalog1 = MissionCatalog(
            host_mission=mission,
            name="CATALOG_WITH_TARGETS",
            description="Catalog with targets",
        )
        catalog2 = MissionCatalog(
            host_mission=mission,
            name="EMPTY_CATALOG",
            description="Empty catalog",
        )
        v2_db.add_all([catalog1, catalog2])
        v2_db.flush()

        # Add targets to catalog1
        for i in range(5):
            target = Target(
                catalog=catalog1,
                name=2000000 + i,
            )
            v2_db.add(target)
        v2_db.commit()

        # Query catalogs with target counts
        from sqlalchemy import func

        catalog_target_counts = (
            v2_db.query(MissionCatalog, func.count(Target.id))
            .outerjoin(Target)
            .group_by(MissionCatalog.id)
            .all()
        )

        for catalog, count in catalog_target_counts:
            if catalog.name == "CATALOG_WITH_TARGETS":
                assert count == 5
            elif catalog.name == "EMPTY_CATALOG":
                assert count == 0
