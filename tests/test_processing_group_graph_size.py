"""Test the graph_size hybrid property for ProcessingGroup."""

import pytest
from sqlalchemy import orm

from lightcurvedb.models.interpretation import (
    InterpretationAssociationTable,
    InterpretationType,
    ProcessingGroup,
)


class TestProcessingGroupGraphSize:
    """Test the graph_size hybrid property functionality."""

    def test_graph_size_empty_group(self, v2_db: orm.Session):
        """Test graph_size returns 0 for a processing group with no associations."""
        # Create a processing group with no associations
        group = ProcessingGroup(
            name="Empty Group",
            description="A group with no associations",
        )
        v2_db.add(group)
        v2_db.flush()

        # Test instance-level property
        assert group.graph_size == 0

        # Test SQL expression
        result = (
            v2_db.query(ProcessingGroup)
            .filter(ProcessingGroup.id == group.id)
            .first()
        )
        assert result.graph_size == 0

    def test_graph_size_linear_chain(self, v2_db: orm.Session):
        """Test graph_size for a linear chain of interpretation types."""
        # Create interpretation types
        type1 = InterpretationType(name="Type1", description="First type")
        type2 = InterpretationType(name="Type2", description="Second type")
        type3 = InterpretationType(name="Type3", description="Third type")
        v2_db.add_all([type1, type2, type3])
        v2_db.flush()

        # Create processing group
        group = ProcessingGroup(
            name="Linear Chain",
            description="A->B->C",
        )
        v2_db.add(group)
        v2_db.flush()

        # Create linear chain: Type1 -> Type2 -> Type3
        assoc1 = InterpretationAssociationTable(
            previous_type_id=type1.id,
            next_type_id=type2.id,
            group_id=group.id,
        )
        assoc2 = InterpretationAssociationTable(
            previous_type_id=type2.id,
            next_type_id=type3.id,
            group_id=group.id,
        )
        v2_db.add_all([assoc1, assoc2])
        v2_db.flush()

        # Test instance-level property
        assert group.graph_size == 3  # Three unique nodes: Type1, Type2, Type3

        # Test SQL expression in query
        result = (
            v2_db.query(ProcessingGroup)
            .filter(ProcessingGroup.graph_size == 3)
            .first()
        )
        assert result.id == group.id

    def test_graph_size_branching_graph(self, v2_db: orm.Session):
        """Test graph_size for a branching graph structure."""
        # Create interpretation types
        types = []
        for i in range(5):
            t = InterpretationType(
                name=f"Type{i+1}",
                description=f"Type {i+1}",
            )
            types.append(t)
        v2_db.add_all(types)
        v2_db.flush()

        # Create processing group
        group = ProcessingGroup(
            name="Branching Graph",
            description="Complex graph with branches",
        )
        v2_db.add(group)
        v2_db.flush()

        # Create branching structure:
        # Type1 -> Type2
        # Type1 -> Type3
        # Type2 -> Type4
        # Type3 -> Type4
        # Type4 -> Type5
        associations = [
            InterpretationAssociationTable(
                previous_type_id=types[0].id,
                next_type_id=types[1].id,
                group_id=group.id,
            ),
            InterpretationAssociationTable(
                previous_type_id=types[0].id,
                next_type_id=types[2].id,
                group_id=group.id,
            ),
            InterpretationAssociationTable(
                previous_type_id=types[1].id,
                next_type_id=types[3].id,
                group_id=group.id,
            ),
            InterpretationAssociationTable(
                previous_type_id=types[2].id,
                next_type_id=types[3].id,
                group_id=group.id,
            ),
            InterpretationAssociationTable(
                previous_type_id=types[3].id,
                next_type_id=types[4].id,
                group_id=group.id,
            ),
        ]
        v2_db.add_all(associations)
        v2_db.flush()

        # Test instance-level property
        assert group.graph_size == 5  # All 5 types are in the graph

        # Test SQL expression with ordering
        results = (
            v2_db.query(ProcessingGroup)
            .order_by(ProcessingGroup.graph_size.desc())
            .all()
        )
        assert results[0].id == group.id

    def test_graph_size_with_multiple_groups(self, v2_db: orm.Session):
        """Test graph_size with multiple processing groups."""
        # Create shared interpretation types
        type1 = InterpretationType(name="Shared1", description="Shared type 1")
        type2 = InterpretationType(name="Shared2", description="Shared type 2")
        type3 = InterpretationType(name="Shared3", description="Shared type 3")
        type4 = InterpretationType(
            name="Unique4", description="Unique to group 2"
        )
        v2_db.add_all([type1, type2, type3, type4])
        v2_db.flush()

        # Create two processing groups
        group1 = ProcessingGroup(name="Group1", description="First group")
        group2 = ProcessingGroup(name="Group2", description="Second group")
        v2_db.add_all([group1, group2])
        v2_db.flush()

        # Group1: Type1 -> Type2
        assoc1 = InterpretationAssociationTable(
            previous_type_id=type1.id,
            next_type_id=type2.id,
            group_id=group1.id,
        )
        v2_db.add(assoc1)

        # Group2: Type1 -> Type3, Type3 -> Type4
        assoc2 = InterpretationAssociationTable(
            previous_type_id=type1.id,
            next_type_id=type3.id,
            group_id=group2.id,
        )
        assoc3 = InterpretationAssociationTable(
            previous_type_id=type3.id,
            next_type_id=type4.id,
            group_id=group2.id,
        )
        v2_db.add_all([assoc2, assoc3])
        v2_db.flush()

        # Test graph sizes
        assert group1.graph_size == 2  # Type1, Type2
        assert group2.graph_size == 3  # Type1, Type3, Type4

        # Test filtering by graph size
        small_graphs = (
            v2_db.query(ProcessingGroup)
            .filter(ProcessingGroup.graph_size < 3)
            .all()
        )
        assert len(small_graphs) == 1
        assert small_graphs[0].id == group1.id

        large_graphs = (
            v2_db.query(ProcessingGroup)
            .filter(ProcessingGroup.graph_size >= 3)
            .all()
        )
        assert len(large_graphs) == 1
        assert large_graphs[0].id == group2.id

    def test_graph_size_sql_expression_in_select(self, v2_db: orm.Session):
        """Test using graph_size in select statements."""
        # Create a simple graph
        type1 = InterpretationType(name="A", description="A")
        type2 = InterpretationType(name="B", description="B")
        v2_db.add_all([type1, type2])
        v2_db.flush()

        group = ProcessingGroup(name="Test", description="Test group")
        v2_db.add(group)
        v2_db.flush()

        assoc = InterpretationAssociationTable(
            previous_type_id=type1.id,
            next_type_id=type2.id,
            group_id=group.id,
        )
        v2_db.add(assoc)
        v2_db.flush()

        # Test selecting with graph_size
        result = (
            v2_db.query(
                ProcessingGroup.name, ProcessingGroup.graph_size.label("size")
            )
            .filter(ProcessingGroup.id == group.id)
            .first()
        )

        assert result.name == "Test"
        assert result.size == 2
