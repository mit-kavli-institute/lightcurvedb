"""Tests for partition DDL primitives and idempotent provisioning.

Every database test runs on ``partitioned_db`` (no DEFAULT partitions)
except where a DEFAULT partition is the point, which use ``v2_db``.
Several tests are characterisation tests: they assert PostgreSQL 14
behaviour that the design depends on, so that an upgrade which changes
it fails here rather than during a replacement.
"""

import pytest
import sqlalchemy as sa
from sqlalchemy import orm

from lightcurvedb.core.partitions import (
    AutocommitRequiredError,
    PartitionError,
    PartitionName,
    StagingShapeMismatchError,
    add_bound_check,
    attach_partition,
    build_partition_indexes,
    check_attachable,
    column_mismatches,
    column_signature,
    create_staging_table,
    detach_partition,
    drop_relation,
    ensure_partition,
    ensure_staging_table,
    find_partition_for_value,
    foreign_key_definitions,
    has_bound_check,
    index_definitions,
    lock_tables,
    mirror_outbound_foreign_keys,
    referenced_tables,
    relation_kind,
    set_local_timeouts,
    verify_relation_shape,
)
from lightcurvedb.models import Observation, Target

pytestmark = pytest.mark.partitioning


@pytest.fixture
def orm_session(partitioned_db):
    """Point the shared sample graph at a database with no defaults."""
    return partitioned_db


def _sql(session: orm.Session, statement: str, **params) -> None:
    session.execute(sa.text(statement), params)


def _constraints(session: orm.Session, relation: str) -> dict[str, str]:
    """``{conname: contype}`` for a relation."""
    rows = session.execute(
        sa.text(
            "SELECT k.conname, k.contype FROM pg_constraint k "
            "JOIN pg_class c ON c.oid = k.conrelid "
            "WHERE c.relname = :relation"
        ),
        {"relation": relation},
    ).all()
    return {r[0]: r[1] for r in rows}


def _insert_dataset_rows(
    session: orm.Session,
    relation: str,
    observation: Observation,
    target: Target,
    value: float,
) -> None:
    _sql(
        session,
        f"INSERT INTO {relation} (observation_id, target_id, "
        "photometric_method_id, processing_method_id, values, errors) "
        "VALUES (:obs, :target, 0, 0, ARRAY[:value]::float8[], NULL)",
        obs=observation.id,
        target=target.id,
        value=value,
    )


class TestCreateStagingTable:
    def test_shape_matches_the_parent(
        self, partitioned_db: orm.Session, sample_observation: Observation
    ):
        conn = partitioned_db.connection()
        create_staging_table(conn, "dataset", "dataset_stage", 7)

        assert relation_kind(conn, "dataset_stage") == "table"
        assert not column_mismatches(
            column_signature(conn, "dataset"),
            column_signature(conn, "dataset_stage"),
            compare_ordinals=True,
        )

    def test_carries_a_valid_bound_check_and_no_indexes(
        self, partitioned_db: orm.Session
    ):
        conn = partitioned_db.connection()
        create_staging_table(conn, "dataset", "dataset_stage", 7)

        assert _constraints(partitioned_db, "dataset_stage") == {
            "dataset_stage_partcheck": "c"
        }
        assert index_definitions(conn, "dataset_stage") == ()

    def test_bound_check_uses_a_bare_integer_literal(
        self, partitioned_db: orm.Session
    ):
        """A cast would yield int48eq and defeat the partition prover."""
        conn = partitioned_db.connection()
        create_staging_table(conn, "dataset", "dataset_stage", 7)

        definition = partitioned_db.execute(
            sa.text(
                "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
                "WHERE conname = 'dataset_stage_partcheck'"
            )
        ).scalar()
        assert definition == "CHECK ((observation_id = 7))"

    def test_copies_no_foreign_keys(self, partitioned_db: orm.Session):
        """``LIKE`` never copies foreign keys, whatever is INCLUDED."""
        conn = partitioned_db.connection()
        create_staging_table(conn, "dataset", "dataset_stage", 7)

        contypes = set(_constraints(partitioned_db, "dataset_stage").values())
        assert "f" not in contypes

    def test_if_not_exists_is_idempotent(self, partitioned_db: orm.Session):
        conn = partitioned_db.connection()
        create_staging_table(conn, "dataset", "dataset_stage", 7)
        create_staging_table(
            conn, "dataset", "dataset_stage", 7, if_not_exists=True
        )
        assert relation_kind(conn, "dataset_stage") == "table"


class TestEnsurePartition:
    def test_creates_the_legacy_unversioned_name(
        self, partitioned_db: orm.Session, sample_observation: Observation
    ):
        conn = partitioned_db.connection()
        name = ensure_partition(conn, "dataset", sample_observation.id)

        assert name.table == f"dataset_obs_{sample_observation.id}"
        live = find_partition_for_value(conn, "dataset", sample_observation.id)
        assert live is not None and live.name == name.table

    def test_second_call_is_a_no_op(
        self, partitioned_db: orm.Session, sample_observation: Observation
    ):
        conn = partitioned_db.connection()
        first = ensure_partition(conn, "dataset", sample_observation.id)
        second = ensure_partition(conn, "dataset", sample_observation.id)
        assert first == second

    def test_refuses_when_another_revision_holds_the_slot(
        self, partitioned_db: orm.Session, sample_observation: Observation
    ):
        conn = partitioned_db.connection()
        ensure_partition(conn, "dataset", sample_observation.id, revision=2)

        with pytest.raises(PartitionError, match="swap"):
            ensure_partition(conn, "dataset", sample_observation.id)

    def test_refuses_to_adopt_a_detached_relation_of_the_same_name(
        self, partitioned_db: orm.Session, sample_observation: Observation
    ):
        conn = partitioned_db.connection()
        create_staging_table(
            conn,
            "dataset",
            f"dataset_obs_{sample_observation.id}",
            sample_observation.id,
        )

        with pytest.raises(PartitionError, match="not attached"):
            ensure_partition(conn, "dataset", sample_observation.id)


class TestEnsureStagingTable:
    def test_creates_then_reuses(self, partitioned_db: orm.Session):
        conn = partitioned_db.connection()
        first = ensure_staging_table(conn, "dataset", 7, 1)
        second = ensure_staging_table(conn, "dataset", 7, 1)

        assert first == second == PartitionName("dataset", 7, 1)
        assert relation_kind(conn, "dataset_obs_7_v1") == "table"

    def test_refuses_a_live_partition(
        self, partitioned_db: orm.Session, sample_observation: Observation
    ):
        conn = partitioned_db.connection()
        ensure_partition(conn, "dataset", sample_observation.id, revision=1)

        with pytest.raises(PartitionError, match="live data"):
            ensure_staging_table(conn, "dataset", sample_observation.id, 1)

    def test_detects_a_table_built_from_an_older_parent(
        self, partitioned_db: orm.Session
    ):
        """The trap CREATE TABLE IF NOT EXISTS sets, caught up front."""
        conn = partitioned_db.connection()
        ensure_staging_table(conn, "dataset", 7, 1)
        _sql(partitioned_db, "ALTER TABLE dataset_obs_7_v1 DROP COLUMN errors")

        with pytest.raises(StagingShapeMismatchError, match="errors missing"):
            ensure_staging_table(conn, "dataset", 7, 1)

    def test_a_dropped_column_on_the_parent_is_not_a_mismatch(
        self, partitioned_db: orm.Session
    ):
        """``LIKE`` renumbers columns, so raw attnums would disagree."""
        _sql(
            partitioned_db,
            "CREATE TABLE widget (obs int NOT NULL, doomed int, keep int) "
            "PARTITION BY LIST (obs)",
        )
        _sql(partitioned_db, "ALTER TABLE widget DROP COLUMN doomed")
        conn = partitioned_db.connection()

        name = ensure_staging_table(conn, "widget", 1, 1).table

        assert not column_mismatches(
            column_signature(conn, "widget"),
            column_signature(conn, name),
            compare_ordinals=True,
        )

    def test_verify_relation_shape_reports_type_drift(
        self, partitioned_db: orm.Session
    ):
        conn = partitioned_db.connection()
        ensure_staging_table(conn, "dataset", 7, 1)
        _sql(
            partitioned_db,
            "ALTER TABLE dataset_obs_7_v1 "
            "ALTER COLUMN target_id TYPE numeric",
        )

        with pytest.raises(StagingShapeMismatchError, match="target_id"):
            verify_relation_shape(conn, "dataset", "dataset_obs_7_v1")


class TestBuildPartitionIndexes:
    def test_reproduces_the_parents_index_set(
        self, partitioned_db: orm.Session
    ):
        conn = partitioned_db.connection()
        create_staging_table(conn, "dataset", "dataset_obs_7_v1", 7)
        created = build_partition_indexes(conn, "dataset", "dataset_obs_7_v1")

        assert set(created) == {
            "dataset_obs_7_v1_pkey",
            "dataset_obs_7_v1_target_idx",
        }
        parent_keys = {spec.key for spec in index_definitions(conn, "dataset")}
        child_keys = {
            spec.key for spec in index_definitions(conn, "dataset_obs_7_v1")
        }
        assert parent_keys == child_keys

    def test_derived_names_match_the_naming_module(
        self, partitioned_db: orm.Session
    ):
        conn = partitioned_db.connection()
        name = PartitionName("dataset", 7, 1)
        create_staging_table(conn, "dataset", name.table, 7)
        created = build_partition_indexes(conn, "dataset", name.table)

        assert name.derived("pkey") in created
        assert name.derived("target_idx") in created

    def test_primary_key_is_a_constraint_not_a_bare_index(
        self, partitioned_db: orm.Session
    ):
        """The difference between a 1 ms attach and an index rebuild."""
        conn = partitioned_db.connection()
        create_staging_table(conn, "dataset", "dataset_obs_7_v1", 7)
        build_partition_indexes(conn, "dataset", "dataset_obs_7_v1")

        specs = {
            spec.name: spec
            for spec in index_definitions(conn, "dataset_obs_7_v1")
        }
        pkey = specs["dataset_obs_7_v1_pkey"]
        assert pkey.constraint_backed
        assert pkey.constraint_type == "p"

    @pytest.mark.usefixtures("dataset_link")
    def test_handles_the_eight_column_link_indexes(
        self, partitioned_db: orm.Session
    ):
        conn = partitioned_db.connection()
        create_staging_table(conn, "datasetlink", "datasetlink_obs_7_v1", 7)
        created = build_partition_indexes(
            conn, "datasetlink", "datasetlink_obs_7_v1"
        )

        assert set(created) == {
            "datasetlink_obs_7_v1_pkey",
            "datasetlink_obs_7_v1_source_idx",
            "datasetlink_obs_7_v1_child_idx",
        }

    def test_second_call_creates_nothing(self, partitioned_db: orm.Session):
        conn = partitioned_db.connection()
        create_staging_table(conn, "dataset", "dataset_obs_7_v1", 7)
        build_partition_indexes(conn, "dataset", "dataset_obs_7_v1")

        assert (
            build_partition_indexes(conn, "dataset", "dataset_obs_7_v1") == ()
        )


class TestArbitraryPartitionedTables:
    """Nothing here is specific to this schema's three tables."""

    @pytest.fixture
    def widget(self, partitioned_db: orm.Session) -> str:
        """A LIST-partitioned table with a UNIQUE constraint."""
        _sql(
            partitioned_db,
            "CREATE TABLE widget ("
            "  obs int NOT NULL, id int NOT NULL, code text, "
            "  CONSTRAINT pk_widget PRIMARY KEY (obs, id), "
            "  CONSTRAINT uq_widget_code UNIQUE (obs, code)"
            ") PARTITION BY LIST (obs)",
        )
        _sql(partitioned_db, "CREATE INDEX ix_widget_code ON widget (code)")
        return "widget"

    def test_indexes_and_constraints_are_all_reproduced(
        self, partitioned_db: orm.Session, widget: str
    ):
        conn = partitioned_db.connection()
        create_staging_table(conn, widget, "widget_obs_1_v1", 1)
        created = build_partition_indexes(conn, widget, "widget_obs_1_v1")

        assert set(created) == {
            "widget_obs_1_v1_pkey",
            "widget_obs_1_v1_code_key",
            "widget_obs_1_v1_code_idx",
        }
        types = _constraints(partitioned_db, "widget_obs_1_v1")
        assert types["widget_obs_1_v1_pkey"] == "p"
        assert types["widget_obs_1_v1_code_key"] == "u"

    def test_the_prepared_table_attaches_as_a_pure_catalog_change(
        self, partitioned_db: orm.Session, widget: str
    ):
        conn = partitioned_db.connection()
        create_staging_table(conn, widget, "widget_obs_1_v1", 1)
        build_partition_indexes(conn, widget, "widget_obs_1_v1")

        report = check_attachable(conn, widget, "widget_obs_1_v1", 1)
        assert report.ok, report.blocking + report.expensive

        attach_partition(
            conn, widget, "widget_obs_1_v1", 1, allow_expensive=False
        )
        live = find_partition_for_value(conn, widget, 1)
        assert live is not None and live.name == "widget_obs_1_v1"


class TestMirrorOutboundForeignKeys:
    def test_mirrors_every_key_of_dataset(self, partitioned_db: orm.Session):
        conn = partitioned_db.connection()
        create_staging_table(conn, "dataset", "dataset_obs_7_v1", 7)
        created = mirror_outbound_foreign_keys(
            conn, "dataset", "dataset_obs_7_v1"
        )

        assert len(created) == 4
        report = check_attachable(conn, "dataset", "dataset_obs_7_v1", 7)
        assert report.missing_foreign_keys == ()

    @pytest.mark.usefixtures("dataset_link")
    def test_skips_keys_pointing_at_a_partitioned_table(
        self, partitioned_db: orm.Session
    ):
        """Section 8.1: pre-creating these would block the swap itself."""
        conn = partitioned_db.connection()
        create_staging_table(conn, "datasetlink", "datasetlink_obs_7_v1", 7)
        created = mirror_outbound_foreign_keys(
            conn, "datasetlink", "datasetlink_obs_7_v1"
        )

        assert created == ()

    @pytest.mark.usefixtures("dataset_link")
    def test_ignores_the_clones_postgresql_makes_per_partition(
        self, partitioned_db: orm.Session, sample_observation: Observation
    ):
        """A referenced partition adds keys that are not the table's own.

        Attaching a partition to ``dataset`` makes PostgreSQL clone
        ``datasetlink``'s keys once per partition, each pointing at
        the concrete partition rather than at the partitioned parent.
        Those clones read as unpartitioned, so without the
        ``conparentid`` filter they are mirrored -- which pins the very
        partition a swap has to detach, and overflows the 63-byte
        identifier limit on the way, since their auto-generated names are
        already at it.

        The plain skip test above passes without the filter only because
        no partition of ``dataset`` exists there.
        """
        conn = partitioned_db.connection()
        key = sample_observation.id
        ensure_partition(conn, "dataset", key)

        assert {
            spec.name for spec in foreign_key_definitions(conn, "datasetlink")
        } == {"fk_datasetlink_source", "fk_datasetlink_child"}

        staged = f"datasetlink_obs_{key}_v1"
        create_staging_table(conn, "datasetlink", staged, key)
        assert mirror_outbound_foreign_keys(conn, "datasetlink", staged) == ()

    @pytest.mark.usefixtures("dataset_link")
    def test_can_be_forced_for_partitioned_referents(
        self, partitioned_db: orm.Session, sample_observation: Observation
    ):
        conn = partitioned_db.connection()
        create_staging_table(conn, "datasetlink", "datasetlink_obs_7_v1", 7)
        created = mirror_outbound_foreign_keys(
            conn,
            "datasetlink",
            "datasetlink_obs_7_v1",
            include_partitioned_referents=True,
        )

        assert len(created) == 2

    def test_second_call_creates_nothing(self, partitioned_db: orm.Session):
        conn = partitioned_db.connection()
        create_staging_table(conn, "dataset", "dataset_obs_7_v1", 7)
        mirror_outbound_foreign_keys(conn, "dataset", "dataset_obs_7_v1")

        assert (
            mirror_outbound_foreign_keys(conn, "dataset", "dataset_obs_7_v1")
            == ()
        )


class TestAttachAndDetach:
    def test_a_fully_prepared_candidate_attaches_cleanly(
        self,
        partitioned_db: orm.Session,
        sample_observation: Observation,
        sample_target: Target,
    ):
        conn = partitioned_db.connection()
        key = sample_observation.id
        name = PartitionName("dataset", key, 1).table
        create_staging_table(conn, "dataset", name, key)
        _insert_dataset_rows(
            partitioned_db, name, sample_observation, sample_target, 1.5
        )
        build_partition_indexes(conn, "dataset", name)
        mirror_outbound_foreign_keys(conn, "dataset", name)

        report = check_attachable(conn, "dataset", name, key)
        assert report.ok, report.expensive + report.blocking

        attach_partition(conn, "dataset", name, key, allow_expensive=False)
        live = find_partition_for_value(conn, "dataset", key)
        assert live is not None and live.name == name

    def test_pre_created_foreign_keys_are_adopted_not_cloned(
        self,
        partitioned_db: orm.Session,
        sample_observation: Observation,
    ):
        """Adoption keeps the same pg_constraint row, now parented."""
        conn = partitioned_db.connection()
        key = sample_observation.id
        name = PartitionName("dataset", key, 1).table
        create_staging_table(conn, "dataset", name, key)
        build_partition_indexes(conn, "dataset", name)
        mirror_outbound_foreign_keys(conn, "dataset", name)

        before = partitioned_db.execute(
            sa.text(
                "SELECT k.oid, k.conparentid FROM pg_constraint k "
                "JOIN pg_class c ON c.oid = k.conrelid "
                "WHERE c.relname = :name AND k.contype = 'f' "
                "ORDER BY k.conname"
            ),
            {"name": name},
        ).all()
        attach_partition(conn, "dataset", name, key)
        after = partitioned_db.execute(
            sa.text(
                "SELECT k.oid, k.conparentid FROM pg_constraint k "
                "JOIN pg_class c ON c.oid = k.conrelid "
                "WHERE c.relname = :name AND k.contype = 'f' "
                "ORDER BY k.conname"
            ),
            {"name": name},
        ).all()

        assert [r[0] for r in before] == [r[0] for r in after]
        assert all(r[1] == 0 for r in before)
        assert all(r[1] != 0 for r in after)

    def test_a_bare_unique_index_is_reported_as_expensive(
        self, partitioned_db: orm.Session
    ):
        """PostgreSQL rebuilds it under ACCESS EXCLUSIVE instead."""
        conn = partitioned_db.connection()
        create_staging_table(conn, "dataset", "dataset_obs_7_v1", 7)
        _sql(
            partitioned_db,
            "CREATE UNIQUE INDEX dataset_obs_7_v1_pkey ON dataset_obs_7_v1 "
            "(observation_id, target_id, photometric_method_id, "
            "processing_method_id)",
        )
        _sql(
            partitioned_db,
            "CREATE INDEX dataset_obs_7_v1_target_idx ON dataset_obs_7_v1 "
            "(target_id)",
        )

        report = check_attachable(conn, "dataset", "dataset_obs_7_v1", 7)
        assert report.unbacked_constraint_indexes == ("dataset_obs_7_v1_pkey",)
        assert not report.ok

    def test_plain_detach_adds_no_check_of_its_own(
        self, partitioned_db: orm.Session, sample_observation: Observation
    ):
        """Characterisation, PostgreSQL 14.

        A partition created by hand carries only the bound PostgreSQL
        derives from ``FOR VALUES IN``, and a plain detach does not
        turn that into a constraint. Re-attaching such a relation means
        re-reading every row -- which is why provisioning adds a real
        CHECK up front.
        """
        conn = partitioned_db.connection()
        key = sample_observation.id
        name = f"dataset_obs_{key}"
        _sql(
            partitioned_db,
            f"CREATE TABLE {name} PARTITION OF dataset "
            f"FOR VALUES IN ({key})",
        )
        detach_partition(conn, "dataset", name)

        assert "c" not in _constraints(partitioned_db, name).values()
        assert not check_attachable(
            conn, "dataset", name, key
        ).has_valid_partition_check

        added = add_bound_check(conn, name, "observation_id", key)
        assert added == f"{name}_partcheck"
        assert check_attachable(
            conn, "dataset", name, key
        ).has_valid_partition_check

    def test_a_provisioned_partition_keeps_its_check_through_detach(
        self, partitioned_db: orm.Session, sample_observation: Observation
    ):
        """So a rollback never pays for a validation scan."""
        conn = partitioned_db.connection()
        key = sample_observation.id
        name = ensure_partition(conn, "dataset", key).table
        assert has_bound_check(conn, name, "observation_id", key)

        detach_partition(conn, "dataset", name)

        assert has_bound_check(conn, name, "observation_id", key)
        assert check_attachable(
            conn, "dataset", name, key
        ).has_valid_partition_check

    def test_add_bound_check_is_idempotent(
        self, partitioned_db: orm.Session, sample_observation: Observation
    ):
        conn = partitioned_db.connection()
        key = sample_observation.id
        name = f"dataset_obs_{key}"
        _sql(
            partitioned_db,
            f"CREATE TABLE {name} PARTITION OF dataset "
            f"FOR VALUES IN ({key})",
        )
        detach_partition(conn, "dataset", name)

        assert add_bound_check(conn, name, "observation_id", key) is not None
        assert add_bound_check(conn, name, "observation_id", key) is None

    def test_detach_concurrently_works_on_an_autocommit_connection(
        self,
        partitioned_db: orm.Session,
        database_engine: sa.Engine,
        sample_observation: Observation,
    ):
        """The positive half of the autocommit guard.

        ``Connection.get_isolation_level()`` reports the server's
        traditional level even under autocommit, so a guard written
        against it would reject every connection and this path would
        be unreachable.
        """
        key = sample_observation.id
        name = ensure_partition(
            partitioned_db.connection(), "dataset", key
        ).table
        partitioned_db.commit()
        partitioned_db.close()

        with database_engine.connect().execution_options(
            isolation_level="AUTOCOMMIT"
        ) as conn:
            detach_partition(conn, "dataset", name, concurrently=True)
            assert find_partition_for_value(conn, "dataset", key) is None
            assert relation_kind(conn, name) == "table"

    def test_detach_concurrently_refuses_inside_a_transaction(
        self, partitioned_db: orm.Session, sample_observation: Observation
    ):
        conn = partitioned_db.connection()
        key = sample_observation.id
        name = ensure_partition(conn, "dataset", key).table

        with pytest.raises(AutocommitRequiredError, match="AUTOCOMMIT"):
            detach_partition(conn, "dataset", name, concurrently=True)

        # The refusal happened before any statement was sent, so the
        # caller's transaction is still usable.
        assert find_partition_for_value(conn, "dataset", key) is not None

    def test_concurrently_and_finalize_are_exclusive(
        self, partitioned_db: orm.Session
    ):
        with pytest.raises(ValueError, match="mutually exclusive"):
            detach_partition(
                partitioned_db.connection(),
                "dataset",
                "whatever",
                concurrently=True,
                finalize=True,
            )


class TestDefaultPartitionInteraction:
    @pytest.fixture
    def orm_session(self, v2_db):
        """This class wants the DEFAULT partitions the module avoids."""
        return v2_db

    def test_a_conflicting_default_row_blocks_the_attach(
        self,
        v2_db: orm.Session,
        sample_observation: Observation,
        sample_target: Target,
    ):
        """Why partition tests use a database with no DEFAULT partitions."""
        conn = v2_db.connection()
        key = sample_observation.id
        _insert_dataset_rows(
            v2_db, "dataset", sample_observation, sample_target, 1.0
        )
        name = PartitionName("dataset", key, 1).table
        create_staging_table(conn, "dataset", name, key)
        build_partition_indexes(conn, "dataset", name)
        mirror_outbound_foreign_keys(conn, "dataset", name)

        report = check_attachable(conn, "dataset", name, key)
        assert report.default_partition_conflicts
        assert not report.ok


class TestDropRelation:
    def test_reports_whether_it_existed(self, partitioned_db: orm.Session):
        conn = partitioned_db.connection()
        create_staging_table(conn, "dataset", "dataset_obs_7_v1", 7)

        assert drop_relation(conn, "dataset_obs_7_v1") is True
        assert drop_relation(conn, "dataset_obs_7_v1") is False


class TestLocksAndTimeouts:
    def test_rejects_a_value_that_is_not_a_duration(
        self, partitioned_db: orm.Session
    ):
        with pytest.raises(ValueError, match="lock_timeout"):
            set_local_timeouts(
                partitioned_db.connection(), lock_timeout="5 seconds'; DROP"
            )

    def test_applies_both_bounds(self, partitioned_db: orm.Session):
        conn = partitioned_db.connection()
        set_local_timeouts(conn, lock_timeout="250ms", statement_timeout="9s")

        assert conn.execute(sa.text("SHOW lock_timeout")).scalar() == "250ms"
        assert conn.execute(sa.text("SHOW statement_timeout")).scalar() == "9s"

    def test_rejects_an_unknown_lock_mode(self, partitioned_db: orm.Session):
        with pytest.raises(ValueError, match="unknown lock mode"):
            lock_tables(partitioned_db.connection(), ["dataset"], "TOTAL")

    @pytest.mark.usefixtures("dataset_link")
    def test_locks_several_tables_at_once(self, partitioned_db: orm.Session):
        conn = partitioned_db.connection()
        lock_tables(conn, ["dataset", "datasetlink"], "ACCESS EXCLUSIVE")

        held = conn.execute(
            sa.text(
                "SELECT count(*) FROM pg_locks l "
                "JOIN pg_class c ON c.oid = l.relation "
                "WHERE c.relname IN ('dataset', 'datasetlink') "
                "  AND l.mode = 'AccessExclusiveLock' AND l.granted"
            )
        ).scalar()
        assert held == 2

    @pytest.mark.usefixtures("dataset_link")
    def test_referenced_tables_are_the_unpartitioned_referents(
        self, partitioned_db: orm.Session
    ):
        found = referenced_tables(
            partitioned_db.connection(), ["dataset", "datasetlink"]
        )
        assert found == (
            "observation",
            "photometric_source",
            "processing_method",
            "target",
        )
