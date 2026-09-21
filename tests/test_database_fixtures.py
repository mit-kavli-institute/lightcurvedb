"""Tests for the database fixtures themselves.

The partition-management work needs a database *without* catch-all
DEFAULT partitions, because a DEFAULT partition makes every subsequent
``ATTACH PARTITION`` scan it under ``ACCESS EXCLUSIVE`` and fail outright
if it holds a conflicting row. These tests pin the difference between the
two database fixtures, the guard that keeps them from being composed by
accident, and the teardown sweep that stops detached partitions leaking
between tests.
"""

import pytest
import sqlalchemy as sa
from sqlalchemy import orm

from lightcurvedb.core.base_model import LCDBModel
from lightcurvedb.models import (
    Observation,
    PhotometricSource,
    ProcessingMethod,
)

from .util import drop_unmanaged_relations, partitioned_table_names


@pytest.fixture
def orm_session(partitioned_db):
    """Point the shared ``sample_*`` object graph at ``partitioned_db``.

    This module-level override is the mechanism partition test modules
    use; exercising it here proves it works.
    """
    return partitioned_db


def _default_partitions(session: orm.Session) -> set[str]:
    """Names of the DEFAULT partitions present in the database."""
    return set(
        session.execute(
            sa.text(
                "SELECT c.relname FROM pg_class c "
                "WHERE c.relname LIKE '%\\_default' AND c.relispartition"
            )
        ).scalars()
    )


class TestPartitionedTableDiscovery:
    def test_names_are_derived_from_metadata(self):
        """The set is discovered, not hardcoded."""
        assert set(partitioned_table_names()) == {
            "dataset",
            "datasethierarchy",
            "target_specific_time",
        }
        assert "observation" not in partitioned_table_names()


class TestDefaultPartitionFixtures:
    """``v2_db`` and ``partitioned_db`` differ only in DEFAULT partitions."""

    def test_v2_db_has_default_partitions(self, v2_db: orm.Session):
        assert _default_partitions(v2_db) == {
            f"{name}_default" for name in partitioned_table_names()
        }

    def test_partitioned_db_has_none(self, partitioned_db: orm.Session):
        assert _default_partitions(partitioned_db) == set()

    def test_partitioned_db_keeps_schema_and_sentinels(
        self, partitioned_db: orm.Session
    ):
        """Dropping the defaults must not cost the rest of the contract."""
        assert partitioned_db.get(PhotometricSource, 0) is not None
        assert partitioned_db.get(ProcessingMethod, 0) is not None
        assert partitioned_db.execute(
            sa.text("SELECT to_regclass('dataset')")
        ).scalar()

    @pytest.mark.partitioning
    def test_attach_needs_no_default_partition_scan(
        self, partitioned_db: orm.Session
    ):
        """The reason ``partitioned_db`` exists.

        A standalone table carrying a matching CHECK attaches as a
        partition. With a DEFAULT partition present PostgreSQL would also
        have to scan it; here there is nothing to scan.
        """
        partitioned_db.execute(
            sa.text(
                "CREATE TABLE dataset_obs_9 ("
                "LIKE dataset INCLUDING ALL EXCLUDING INDEXES, "
                "CONSTRAINT dataset_obs_9_check CHECK (observation_id = 9))"
            )
        )
        partitioned_db.execute(
            sa.text(
                "ALTER TABLE dataset ATTACH PARTITION dataset_obs_9 "
                "FOR VALUES IN (9)"
            )
        )
        partitioned_db.commit()

        bound = partitioned_db.execute(
            sa.text(
                "SELECT pg_get_expr(c.relpartbound, c.oid) FROM pg_class c "
                "WHERE c.relname = 'dataset_obs_9'"
            )
        ).scalar()
        assert bound == "FOR VALUES IN (9)"


class TestPartitionedDbContract:
    """``partitioned_db`` refuses to be composed with the DEFAULT partitions.

    The realistic way to get here by accident is a module that requests
    ``partitioned_db`` but forgets to override ``orm_session``: the
    ``sample_*`` graph then resolves through ``v2_db`` and creates the
    defaults anyway, silently defeating the fixture.
    """

    @pytest.mark.xfail(
        strict=True,
        reason="partitioned_db must fail setup when default_partitions "
        "is also requested, even transitively",
    )
    def test_composition_with_defaults_is_rejected(
        self, partitioned_db: orm.Session, default_partitions
    ):
        pass  # pragma: no cover -- setup must raise before this body runs


class TestSampleFixtureIndirection:
    """``sample_*`` fixtures follow whatever ``orm_session`` resolves to."""

    def test_sample_graph_lands_in_partitioned_db(
        self,
        partitioned_db: orm.Session,
        sample_observation: Observation,
    ):
        # The object graph was built...
        assert sample_observation.id is not None
        assert (
            partitioned_db.get(Observation, sample_observation.id) is not None
        )
        # ...in a database with no DEFAULT partitions, i.e. the override
        # took effect rather than the conftest default of v2_db.
        assert _default_partitions(partitioned_db) == set()


class _LateRegisteredModel(LCDBModel):
    """A model added to the shared metadata by importing this module.

    Stands in for the real case: ``test_dataset_relationships`` declares
    ``Orbit`` this way. Any sweep that snapshots the managed set before
    the module is imported would wrongly drop this table.
    """

    __tablename__ = "late_registered_model"

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)


class TestUnmanagedRelationSweep:
    """Detached partitions must not leak into the next test."""

    @pytest.mark.partitioning
    def test_sweep_drops_detached_leftovers_only(
        self, partitioned_db: orm.Session
    ):
        partitioned_db.execute(
            sa.text("CREATE TABLE dataset_obs_11 (LIKE dataset)")
        )
        partitioned_db.commit()
        engine = partitioned_db.get_bind()

        assert "dataset_obs_11" not in LCDBModel.metadata.tables
        dropped = drop_unmanaged_relations(engine)

        assert dropped == ["dataset_obs_11"]
        with engine.connect() as conn:
            assert (
                conn.execute(
                    sa.text("SELECT to_regclass('dataset_obs_11')")
                ).scalar()
                is None
            )
            # A mapped table is untouched.
            assert conn.execute(
                sa.text("SELECT to_regclass('dataset')")
            ).scalar()

    def test_sweep_reads_metadata_lazily(self, partitioned_db: orm.Session):
        """The managed set must be read at call time.

        ``_LateRegisteredModel`` above joins the metadata only when this
        module is imported -- after ``conftest`` is. It is created by
        ``create_all`` like any other mapped table, and the sweep must
        leave it alone.
        """
        assert "late_registered_model" in LCDBModel.metadata.tables
        engine = partitioned_db.get_bind()

        dropped = drop_unmanaged_relations(engine)

        assert "late_registered_model" not in dropped
        with engine.connect() as conn:
            assert conn.execute(
                sa.text("SELECT to_regclass('late_registered_model')")
            ).scalar()
