"""Test Instrument model functionality."""

import uuid

import pytest
from sqlalchemy import exc, orm

from lightcurvedb.models import Instrument, Observation


class TestInstrumentBasics:
    """Test basic Instrument model functionality."""

    def test_create_instrument(self, v2_db: orm.Session):
        """Test creating a basic instrument."""
        instrument = Instrument(
            name="TESS Camera 1",
            properties={"type": "camera", "manufacturer": "MIT"},
        )
        v2_db.add(instrument)
        v2_db.commit()

        # Verify creation
        assert instrument.id is not None
        assert isinstance(instrument.id, uuid.UUID)
        assert instrument.name == "TESS Camera 1"
        assert instrument.properties == {
            "type": "camera",
            "manufacturer": "MIT",
        }
        assert instrument.parent_id is None

    def test_create_instrument_with_uuid(self, v2_db: orm.Session):
        """Test creating an instrument with explicit UUID."""
        test_uuid = uuid.uuid4()
        instrument = Instrument(
            id=test_uuid,
            name="Custom UUID Instrument",
            properties={},
        )
        v2_db.add(instrument)
        v2_db.commit()

        assert instrument.id == test_uuid

    def test_instrument_properties_json(self, v2_db: orm.Session):
        """Test instrument properties JSON field behavior."""
        # Test with complex nested properties
        complex_properties = {
            "type": "ccd",
            "specs": {
                "resolution": "2048x2048",
                "pixel_size": 15.0,
                "quantum_efficiency": 0.95,
            },
            "calibration": {
                "dark_current": 0.01,
                "read_noise": 3.5,
            },
            "array_data": [1, 2, 3, 4, 5],
        }

        instrument = Instrument(
            name="CCD Detector",
            properties=complex_properties,
        )
        v2_db.add(instrument)
        v2_db.commit()

        # Refresh from database
        v2_db.refresh(instrument)
        assert instrument.properties == complex_properties
        assert instrument.properties["specs"]["pixel_size"] == 15.0
        assert instrument.properties["array_data"] == [1, 2, 3, 4, 5]

    def test_instrument_empty_properties(self, v2_db: orm.Session):
        """Test instrument with empty properties dictionary."""
        instrument = Instrument(
            name="Simple Instrument",
            properties={},
        )
        v2_db.add(instrument)
        v2_db.commit()

        assert instrument.properties == {}


class TestInstrumentRelationships:
    """Test Instrument relationships."""

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

    def test_instrument_nested_hierarchy(self, v2_db: orm.Session):
        """Test multi-level instrument hierarchy."""
        # Create spacecraft -> camera -> ccd hierarchy
        spacecraft = Instrument(
            name="TESS Spacecraft",
            properties={"type": "spacecraft"},
        )
        v2_db.add(spacecraft)
        v2_db.flush()

        camera = Instrument(
            name="Camera 1",
            properties={"type": "camera"},
            parent=spacecraft,
        )
        v2_db.add(camera)
        v2_db.flush()

        ccd1 = Instrument(
            name="CCD 1",
            properties={"type": "ccd", "number": 1},
            parent=camera,
        )
        ccd2 = Instrument(
            name="CCD 2",
            properties={"type": "ccd", "number": 2},
            parent=camera,
        )
        v2_db.add_all([ccd1, ccd2])
        v2_db.commit()

        # Verify hierarchy
        assert len(spacecraft.children) == 1
        assert camera in spacecraft.children
        assert len(camera.children) == 2
        assert ccd1 in camera.children
        assert ccd2 in camera.children
        assert ccd1.parent.parent == spacecraft

    def test_instrument_observations_relationship(self, v2_db: orm.Session):
        """Test Instrument -> Observations relationship."""
        import numpy as np

        instrument = Instrument(
            name="Test Camera",
            properties={"type": "camera"},
        )
        v2_db.add(instrument)
        v2_db.flush()

        # Create multiple observations
        obs1 = Observation(
            instrument=instrument,
            cadence_reference=np.array([1, 2, 3], dtype=np.int64),
        )
        obs2 = Observation(
            instrument=instrument,
            cadence_reference=np.array([4, 5, 6], dtype=np.int64),
        )
        v2_db.add_all([obs1, obs2])
        v2_db.commit()

        # Test instrument -> observations relationship
        assert len(instrument.observations) == 2
        assert obs1 in instrument.observations
        assert obs2 in instrument.observations

        # Test observations -> instrument relationship
        assert obs1.instrument == instrument
        assert obs2.instrument == instrument


class TestInstrumentConstraints:
    """Test Instrument database constraints."""

    def test_instrument_duplicate_name_allowed(self, v2_db: orm.Session):
        """Test that duplicate names are allowed (no unique constraint)."""
        # Create first instrument
        instrument1 = Instrument(
            name="Duplicate Instrument",
            properties={},
        )
        v2_db.add(instrument1)
        v2_db.commit()

        # Create second instrument with same name - should succeed
        instrument2 = Instrument(
            name="Duplicate Instrument",  # Same name
            properties={"different": "properties"},
        )
        v2_db.add(instrument2)
        v2_db.commit()  # Should succeed

        # Verify both exist
        instruments = (
            v2_db.query(Instrument)
            .filter_by(name="Duplicate Instrument")
            .all()
        )
        assert len(instruments) == 2
        assert instrument1 in instruments
        assert instrument2 in instruments

    def test_instrument_foreign_key_constraint_parent(
        self, v2_db: orm.Session
    ):
        """Test foreign key constraint for invalid parent_id."""
        non_existent_uuid = uuid.uuid4()
        instrument = Instrument(
            name="Orphan Instrument",
            properties={},
            parent_id=non_existent_uuid,  # Non-existent parent
        )
        v2_db.add(instrument)

        with pytest.raises(exc.IntegrityError):
            v2_db.commit()
        v2_db.rollback()

    def test_instrument_cascade_behavior(self, v2_db: orm.Session):
        """Test cascade behavior when deleting parent instrument."""
        # Create parent with children
        parent = Instrument(
            name="Parent Instrument",
            properties={"type": "parent"},
        )
        v2_db.add(parent)
        v2_db.flush()

        child = Instrument(
            name="Child Instrument",
            properties={"type": "child"},
            parent=parent,
        )
        v2_db.add(child)
        v2_db.commit()

        parent_id = parent.id
        child_id = child.id

        # Delete parent - child's parent_id should be set to NULL
        v2_db.delete(parent)
        v2_db.commit()

        # Verify parent is deleted
        assert v2_db.query(Instrument).filter_by(id=parent_id).first() is None

        # Verify child still exists but parent_id is NULL
        child_check = v2_db.query(Instrument).filter_by(id=child_id).first()
        assert child_check is not None
        assert child_check.parent_id is None
        assert child_check.parent is None

    def test_instrument_required_fields(self, v2_db: orm.Session):
        """Test required fields for Instrument."""
        # Try to create instrument without name
        instrument = Instrument(
            properties={},
        )
        v2_db.add(instrument)

        with pytest.raises(exc.IntegrityError):
            v2_db.commit()
        v2_db.rollback()


class TestInstrumentQueries:
    """Test various query patterns for Instrument model."""

    def test_query_instruments_by_property(self, v2_db: orm.Session):
        """Test querying instruments by JSON properties."""
        # Create instruments with different properties
        camera1 = Instrument(
            name="Camera 1",
            properties={"type": "camera", "manufacturer": "MIT"},
        )
        camera2 = Instrument(
            name="Camera 2",
            properties={"type": "camera", "manufacturer": "Teledyne"},
        )
        ccd = Instrument(
            name="CCD 1",
            properties={"type": "ccd", "manufacturer": "Teledyne"},
        )
        v2_db.add_all([camera1, camera2, ccd])
        v2_db.commit()

        # Query by property type
        cameras = (
            v2_db.query(Instrument)
            .filter(Instrument.properties["type"].astext == "camera")
            .all()
        )
        assert len(cameras) == 2
        assert camera1 in cameras
        assert camera2 in cameras

        # Query by manufacturer
        teledyne_instruments = (
            v2_db.query(Instrument)
            .filter(Instrument.properties["manufacturer"].astext == "Teledyne")
            .all()
        )
        assert len(teledyne_instruments) == 2
        assert camera2 in teledyne_instruments
        assert ccd in teledyne_instruments

    def test_query_instrument_hierarchy(self, v2_db: orm.Session):
        """Test querying instrument hierarchies."""
        # Create hierarchy
        spacecraft = Instrument(
            name="Spacecraft",
            properties={"type": "spacecraft"},
        )
        v2_db.add(spacecraft)
        v2_db.flush()

        cameras = []
        for i in range(4):
            camera = Instrument(
                name=f"Camera {i+1}",
                properties={"type": "camera", "number": i + 1},
                parent=spacecraft,
            )
            cameras.append(camera)
        v2_db.add_all(cameras)
        v2_db.commit()

        # Query all root instruments (no parent)
        roots = (
            v2_db.query(Instrument)
            .filter(Instrument.parent_id.is_(None))
            .all()
        )
        assert len(roots) == 1
        assert spacecraft in roots

        # Query all children of spacecraft
        children = (
            v2_db.query(Instrument)
            .filter(Instrument.parent_id == spacecraft.id)
            .all()
        )
        assert len(children) == 4
        for camera in cameras:
            assert camera in children
