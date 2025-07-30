"""Test Mission model functionality."""

import uuid

import pytest
from sqlalchemy import delete, exc, orm

from lightcurvedb.models import Mission, MissionCatalog


class TestMissionBasics:
    """Test basic Mission model functionality."""

    def test_create_mission(self, v2_db: orm.Session):
        """Test creating a basic mission."""
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
        v2_db.commit()

        # Verify creation
        assert mission.id is not None
        assert isinstance(mission.id, uuid.UUID)
        assert mission.name == "TESS"
        assert mission.description == "Transiting Exoplanet Survey Satellite"
        assert mission.time_unit == "day"
        assert mission.time_epoch == 2457000.0
        assert mission.time_epoch_scale == "tdb"
        assert mission.time_epoch_format == "jd"
        assert mission.time_format_name == "btjd"

    def test_mission_time_fields_validation(self, v2_db: orm.Session):
        """Test various time-related field values."""
        mission = Mission(
            id=uuid.uuid4(),
            name="Kepler",
            description="Kepler Space Telescope",
            time_unit="second",  # Different time unit
            time_epoch=2454833.0,  # Kepler epoch
            time_epoch_scale="utc",  # Different scale
            time_epoch_format="mjd",  # Modified Julian Date
            time_format_name="kbjd",  # Kepler Barycentric Julian Date
        )
        v2_db.add(mission)
        v2_db.commit()

        assert mission.time_unit == "second"
        assert mission.time_epoch_scale == "utc"
        assert mission.time_epoch_format == "mjd"

    def test_mission_minimal_fields(self, v2_db: orm.Session):
        """Test creating mission with only required fields."""
        mission = Mission(
            id=uuid.uuid4(),
            name="MINIMAL_MISSION",
            description="Minimal mission for testing",
            time_unit="day",
            time_epoch=0.0,
            time_epoch_scale="tdb",
            time_epoch_format="jd",
            time_format_name="test_time",
        )
        v2_db.add(mission)
        v2_db.commit()

        assert mission.id is not None
        assert mission.name == "MINIMAL_MISSION"

    def test_mission_long_description(self, v2_db: orm.Session):
        """Test mission with very long description."""
        long_description = "A" * 1000  # 1000 character description
        mission = Mission(
            id=uuid.uuid4(),
            name="LONG_DESC_MISSION",
            description=long_description,
            time_unit="day",
            time_epoch=0.0,
            time_epoch_scale="tdb",
            time_epoch_format="jd",
            time_format_name="test",
        )
        v2_db.add(mission)
        v2_db.commit()

        assert len(mission.description) == 1000
        assert mission.description == long_description


class TestMissionRelationships:
    """Test Mission relationships."""

    def test_mission_catalog_relationships(self, v2_db: orm.Session):
        """Test Mission <-> MissionCatalog relationships."""
        # Create mission
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

        # Create catalogs
        catalog1 = MissionCatalog(
            host_mission_id=mission.id,
            name="TIC",
            description="TESS Input Catalog",
        )
        catalog2 = MissionCatalog(
            host_mission_id=mission.id,
            name="CTL",
            description="Candidate Target List",
        )
        v2_db.add_all([catalog1, catalog2])
        v2_db.flush()

        # Test mission -> catalogs relationship
        assert len(mission.catalogs) == 2
        assert catalog1 in mission.catalogs
        assert catalog2 in mission.catalogs

        # Test catalogs -> mission relationship
        assert catalog1.host_mission == mission
        assert catalog2.host_mission == mission

    def test_mission_multiple_catalogs(self, v2_db: orm.Session):
        """Test mission with many catalogs."""
        mission = Mission(
            id=uuid.uuid4(),
            name="MULTI_CATALOG_MISSION",
            description="Mission with multiple catalogs",
            time_unit="day",
            time_epoch=0.0,
            time_epoch_scale="tdb",
            time_epoch_format="jd",
            time_format_name="test",
        )
        v2_db.add(mission)
        v2_db.flush()

        # Create multiple catalogs
        catalogs = []
        for i in range(5):
            catalog = MissionCatalog(
                host_mission=mission,
                name=f"CATALOG_{i}",
                description=f"Test catalog {i}",
            )
            catalogs.append(catalog)
        v2_db.add_all(catalogs)
        v2_db.commit()

        # Verify all catalogs are associated
        assert len(mission.catalogs) == 5
        for catalog in catalogs:
            assert catalog in mission.catalogs
            assert catalog.host_mission == mission


class TestMissionConstraints:
    """Test Mission database constraints."""

    def test_mission_unique_name_constraint(self, v2_db: orm.Session):
        """Test unique constraint on mission name."""
        # Create first mission
        mission1 = Mission(
            id=uuid.uuid4(),
            name="UNIQUE_MISSION",
            description="First mission",
            time_unit="day",
            time_epoch=0.0,
            time_epoch_scale="tdb",
            time_epoch_format="jd",
            time_format_name="test",
        )
        v2_db.add(mission1)
        v2_db.commit()

        # Try to create duplicate with same name
        mission2 = Mission(
            id=uuid.uuid4(),  # Different ID
            name="UNIQUE_MISSION",  # Same name
            description="Second mission",
            time_unit="hour",
            time_epoch=1.0,
            time_epoch_scale="utc",
            time_epoch_format="mjd",
            time_format_name="test2",
        )
        v2_db.add(mission2)

        with pytest.raises(exc.IntegrityError):
            v2_db.commit()
        v2_db.rollback()

    def test_mission_required_fields(self, v2_db: orm.Session):
        """Test required fields for Mission."""
        # Try to create mission without name
        mission = Mission(
            id=uuid.uuid4(),
            # name is missing
            description="Invalid mission",
            time_unit="day",
            time_epoch=0.0,
            time_epoch_scale="tdb",
            time_epoch_format="jd",
            time_format_name="test",
        )
        v2_db.add(mission)

        with pytest.raises(exc.IntegrityError):
            v2_db.commit()
        v2_db.rollback()

        # Try without description
        mission2 = Mission(
            id=uuid.uuid4(),
            name="INVALID_MISSION_2",
            # description is missing
            time_unit="day",
            time_epoch=0.0,
            time_epoch_scale="tdb",
            time_epoch_format="jd",
            time_format_name="test2",
        )
        v2_db.add(mission2)

        with pytest.raises(exc.IntegrityError):
            v2_db.commit()
        v2_db.rollback()

    def test_mission_cascade_delete_catalogs(self, v2_db: orm.Session):
        """Test cascade deletion of catalogs when mission is deleted."""
        mission = Mission(
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

        assert mission.id is not None

        catalog = MissionCatalog(
            host_mission_id=mission.id,
            name="CASCADE_TEST_CATALOG",
            description="Catalog for cascade test",
        )
        v2_db.add(catalog)
        v2_db.flush()

        mission_id = mission.id
        catalog_id = catalog.id

        # Delete mission
        v2_db.execute(delete(Mission).where(Mission.id == mission_id))

        # Verify mission is deleted
        assert v2_db.query(Mission).filter_by(id=mission_id).first() is None

        # Verify catalog is also deleted (cascade)
        assert (
            v2_db.query(MissionCatalog).filter_by(id=catalog_id).first()
            is None
        )

    def test_mission_uuid_primary_key(self, v2_db: orm.Session):
        """Test UUID primary key behavior."""
        # Create mission without specifying ID
        mission = Mission(
            # id not specified - should be auto-generated
            name="AUTO_UUID_MISSION",
            description="Mission with auto-generated UUID",
            time_unit="day",
            time_epoch=0.0,
            time_epoch_scale="tdb",
            time_epoch_format="jd",
            time_format_name="test",
        )
        v2_db.add(mission)
        v2_db.commit()

        # Verify UUID was auto-generated
        assert mission.id is not None
        assert isinstance(mission.id, uuid.UUID)

        # Try to create mission with duplicate UUID
        duplicate_mission = Mission(
            id=mission.id,  # Same UUID
            name="DUPLICATE_UUID_MISSION",
            description="Mission with duplicate UUID",
            time_unit="day",
            time_epoch=0.0,
            time_epoch_scale="tdb",
            time_epoch_format="jd",
            time_format_name="test",
        )
        v2_db.add(duplicate_mission)

        with pytest.raises(exc.IntegrityError):
            v2_db.flush()
        v2_db.rollback()


class TestMissionQueries:
    """Test various query patterns for Mission model."""

    def test_query_mission_by_name(self, v2_db: orm.Session):
        """Test querying missions by name."""
        # Create multiple missions
        missions = []
        for name in ["TESS", "Kepler", "JWST", "Hubble"]:
            mission = Mission(
                id=uuid.uuid4(),
                name=name,
                description=f"{name} Space Telescope",
                time_unit="day",
                time_epoch=0.0,
                time_epoch_scale="tdb",
                time_epoch_format="jd",
                time_format_name=name.lower(),
            )
            missions.append(mission)
        v2_db.add_all(missions)
        v2_db.commit()

        # Query by exact name
        tess = v2_db.query(Mission).filter_by(name="TESS").first()
        assert tess is not None
        assert tess.name == "TESS"

        # Query with LIKE pattern
        space_telescopes = (
            v2_db.query(Mission)
            .filter(Mission.description.like("%Space Telescope%"))
            .all()
        )
        assert len(space_telescopes) == 4

    def test_query_mission_with_catalog_count(self, v2_db: orm.Session):
        """Test querying missions with catalog counts."""
        # Create missions with different numbers of catalogs
        mission1 = Mission(
            id=uuid.uuid4(),
            name="MISSION_WITH_CATALOGS",
            description="Mission with catalogs",
            time_unit="day",
            time_epoch=0.0,
            time_epoch_scale="tdb",
            time_epoch_format="jd",
            time_format_name="test1",
        )
        mission2 = Mission(
            id=uuid.uuid4(),
            name="MISSION_NO_CATALOGS",
            description="Mission without catalogs",
            time_unit="day",
            time_epoch=0.0,
            time_epoch_scale="tdb",
            time_epoch_format="jd",
            time_format_name="test2",
        )
        v2_db.add_all([mission1, mission2])
        v2_db.flush()

        # Add catalogs to mission1
        for i in range(3):
            catalog = MissionCatalog(
                host_mission=mission1,
                name=f"CATALOG_{i}",
                description=f"Catalog {i}",
            )
            v2_db.add(catalog)
        v2_db.commit()

        # Query missions with catalogs
        missions_with_catalogs = (
            v2_db.query(Mission).join(MissionCatalog).distinct().all()
        )
        assert len(missions_with_catalogs) == 1
        assert mission1 in missions_with_catalogs

        # Query missions without catalogs using outer join
        from sqlalchemy import func

        mission_catalog_counts = (
            v2_db.query(Mission, func.count(MissionCatalog.id))
            .outerjoin(MissionCatalog)
            .group_by(Mission.id)
            .all()
        )

        for mission, count in mission_catalog_counts:
            if mission.name == "MISSION_WITH_CATALOGS":
                assert count == 3
            elif mission.name == "MISSION_NO_CATALOGS":
                assert count == 0
