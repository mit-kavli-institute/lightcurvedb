"""Simple test for graph_size hybrid property."""

from sqlalchemy import orm

from lightcurvedb.models.interpretation import (
    InterpretationAssociationTable,
    InterpretationType,
    ProcessingGroup,
)


def test_graph_size_basic(v2_db: orm.Session):
    """Test basic graph_size functionality."""
    # Create types
    t1 = InterpretationType(name="T1", description="Type 1")
    t2 = InterpretationType(name="T2", description="Type 2")
    v2_db.add_all([t1, t2])
    v2_db.flush()

    # Create group
    group = ProcessingGroup(name="Test", description="Test group")
    v2_db.add(group)
    v2_db.flush()

    # Empty group should have size 0
    assert group.graph_size == 0

    # Add association
    assoc = InterpretationAssociationTable(
        previous_type_id=t1.id,
        next_type_id=t2.id,
        group_id=group.id,
    )
    v2_db.add(assoc)
    v2_db.flush()

    # Should have 2 unique types
    assert group.graph_size == 2
