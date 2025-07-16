"""Test SQLAlchemy relationships for all models in the lightcurvedb package."""

import uuid

import numpy as np
from sqlalchemy import orm

from lightcurvedb.models import (
    Instrument,
    Mission,
    MissionCatalog,
    Observation,
    Target,
    TargetSpecificTime,
)


class TestModelRelationships:
    """Test all SQLAlchemy relationships are properly configured."""

    def test_instrument_self_referential_relationships(
        self, v2_db: orm.Session
    ):
        """Test Instrument parent/children self-referential relationships."""
        # Create parent instrument
        parent = Instrument(
            name="TESS",
            properties={"type": "spacecraft"},
        )
        v2_db.add(parent)
        v2_db.flush()

        # Create child instruments
        child1 = Instrument(
            name="Camera 1",
            properties={"type": "camera"},
            parent_id=parent.id,
        )
        child2 = Instrument(
            name="Camera 2",
            properties={"type": "camera"},
            parent_id=parent.id,
        )
        v2_db.add_all([child1, child2])
        v2_db.flush()

        # Test parent -> children relationship
        assert len(parent.children) == 2
        assert child1 in parent.children
        assert child2 in parent.children

        # Test children -> parent relationship
        assert child1.parent == parent
        assert child2.parent == parent

    def test_observation_instrument_relationship(self, v2_db: orm.Session):
        """Test Observation <-> Instrument many-to-many relationship."""
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

        # Test observation -> instruments relationship
        assert observation.instrument == instrument1

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

    def test_target_catalog_relationship(self, v2_db: orm.Session):
        """Test Target <-> MissionCatalog relationship."""
        # Create mission and catalog
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

    def test_target_specific_time_relationships(self, v2_db: orm.Session):
        """
        Test TargetSpecificTime relationships with Target and Observation.
        """
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
        v2_db.add(mission)
        v2_db.flush()

        catalog = MissionCatalog(
            host_mission_id=mission.id,
            name="TIC",
            description="Test catalog",
        )
        v2_db.add(catalog)
        v2_db.flush()

        instrument = Instrument(
            name="Test CCD 1",
            properties={"type": "ccd"},
        )
        v2_db.add(instrument)

        target = Target(catalog_id=catalog.id, name=12345678)
        v2_db.add(target)
        v2_db.flush()

        observation = Observation(
            instrument=instrument,
            cadence_reference=np.array([1, 2, 3], dtype=np.int64),
        )
        v2_db.add(observation)
        v2_db.flush()

        # Create target specific time
        tst = TargetSpecificTime(
            target_id=target.id,
            observation_id=observation.id,
            barycentric_julian_dates=np.array(
                [2458000.0, 2458001.0], dtype=np.float64
            ),
        )
        v2_db.add(tst)
        v2_db.flush()

        # Test tst -> target relationship
        assert tst.target == target

        # Test tst -> observation relationship
        assert tst.observation == observation

        # Test target -> target_specific_times relationship
        assert len(target.target_specific_times) == 1
        assert tst in target.target_specific_times

        # Test observation -> target_specific_times relationship
        assert len(observation.target_specific_times) == 1
        assert tst in observation.target_specific_times
