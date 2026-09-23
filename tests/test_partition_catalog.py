"""Tests for read-only partition introspection.

Every database test uses ``partitioned_db`` (no DEFAULT partitions) except
where a DEFAULT partition is the point, which use ``v2_db``. Partitions are
provisioned with raw DDL: the DDL primitives arrive in a later PR and this
module must not depend on them.
"""

import pytest
import sqlalchemy as sa
from sqlalchemy import orm

from lightcurvedb.core.partitions import (
    NotAttachableError,
    NotPartitionedError,
    RelationNotFoundError,
    UnsupportedPartitionStrategyError,
    check_attachable,
    default_partition_of,
    find_partition_for_value,
    list_table_partitions,
    parse_list_bound,
    partition_strategy,
    relation_kind,
    require_list_partitioned,
    resolve_table_name,
)
from lightcurvedb.models import DataSet

pytestmark = pytest.mark.partitioning

# The four outbound foreign keys on ``dataset``, in the form
# ``pg_get_constraintdef`` renders them, so a fully prepared candidate can
# carry structurally identical constraints.
DATASET_FKS = (
    "FOREIGN KEY (observation_id) REFERENCES observation(id) "
    "ON DELETE CASCADE",
    "FOREIGN KEY (target_id) REFERENCES target(id) ON DELETE CASCADE",
    "FOREIGN KEY (photometric_method_id) REFERENCES photometric_source(id) "
    "ON DELETE RESTRICT",
    "FOREIGN KEY (processing_method_id) REFERENCES processing_method(id) "
    "ON DELETE RESTRICT",
)


def _sql(session: orm.Session, statement: str, **params) -> None:
    session.execute(sa.text(statement), params)
    session.commit()


def _create_partition(session: orm.Session, table: str, value: int) -> str:
    name = f"{table}_obs_{value}"
    _sql(
        session,
        f"CREATE TABLE {name} PARTITION OF {table} FOR VALUES IN ({value})",
    )
    return name


def _staged_candidate(
    session: orm.Session,
    value: int,
    *,
    check: bool = True,
    primary_key: bool = True,
    target_index: bool = True,
    foreign_keys: bool = True,
) -> str:
    """Build a ``dataset`` staging table with the chosen preparation."""
    name = f"dataset_stage_{value}"
    check_clause = (
        f", CONSTRAINT {name}_partcheck CHECK (observation_id = {value})"
        if check
        else ""
    )
    _sql(
        session,
        f"CREATE TABLE {name} (LIKE dataset INCLUDING ALL EXCLUDING INDEXES"
        f"{check_clause})",
    )
    if primary_key:
        _sql(
            session,
            f"ALTER TABLE {name} ADD CONSTRAINT {name}_pkey PRIMARY KEY "
            "(observation_id, target_id, photometric_method_id, "
            "processing_method_id)",
        )
    if target_index:
        _sql(session, f"CREATE INDEX {name}_target_idx ON {name} (target_id)")
    if foreign_keys:
        for i, fk in enumerate(DATASET_FKS):
            _sql(
                session, f"ALTER TABLE {name} ADD CONSTRAINT {name}_fk{i} {fk}"
            )
    return name


class TestResolveTableName:
    """Pure; no database."""

    def test_accepts_str(self):
        assert resolve_table_name("dataset") == "dataset"

    def test_accepts_table(self):
        assert resolve_table_name(DataSet.__table__) == "dataset"

    def test_accepts_mapped_class_without_importing_models(self):
        """Duck-typed on ``__table__`` so the package stays model-free."""
        assert resolve_table_name(DataSet) == "dataset"

    def test_rejects_other_types(self):
        with pytest.raises(TypeError):
            resolve_table_name(42)


class TestParseListBound:
    """Pure; no database."""

    @pytest.mark.parametrize(
        ("bound", "expected"),
        [
            ("FOR VALUES IN (5)", (5,)),
            ("FOR VALUES IN (5, 6, 7)", (5, 6, 7)),
            ("DEFAULT", ()),
            (None, ()),
            ("FOR VALUES IN ('a')", ()),
            ("FOR VALUES FROM (1) TO (10)", ()),
        ],
    )
    def test_parse(self, bound, expected):
        assert parse_list_bound(bound) == expected


class TestRelationKind:
    def test_kinds(self, partitioned_db: orm.Session):
        conn = partitioned_db.connection()
        assert relation_kind(conn, "dataset") == "partitioned table"
        assert relation_kind(conn, "observation") == "table"
        assert relation_kind(conn, "no_such_relation") is None

    def test_partition_is_a_plain_table(self, partitioned_db: orm.Session):
        name = _create_partition(partitioned_db, "dataset", 5)
        assert relation_kind(partitioned_db.connection(), name) == "table"


class TestPartitionStrategy:
    def test_list_partitioned_tables(self, partitioned_db: orm.Session):
        conn = partitioned_db.connection()
        ds = partition_strategy(conn, "dataset")
        assert ds is not None
        assert ds.strategy == "list"
        assert ds.key_columns == ("observation_id",)
        assert ds.key_column == "observation_id"
        assert ds.default_partition_oid is None

        dh = partition_strategy(conn, "datasethierarchy")
        assert dh is not None
        assert dh.key_columns == ("source_observation_id",)

    def test_unpartitioned_is_none(self, partitioned_db: orm.Session):
        assert (
            partition_strategy(partitioned_db.connection(), "observation")
            is None
        )

    def test_missing_raises(self, partitioned_db: orm.Session):
        with pytest.raises(RelationNotFoundError):
            partition_strategy(partitioned_db.connection(), "no_such_relation")

    def test_default_partition_oid_reported(self, v2_db: orm.Session):
        conn = v2_db.connection()
        strategy = partition_strategy(conn, "dataset")
        assert strategy is not None
        assert strategy.default_partition_oid is not None
        assert default_partition_of(conn, "dataset").oid == (
            strategy.default_partition_oid
        )

    def test_require_list_rejects_unpartitioned(
        self, partitioned_db: orm.Session
    ):
        with pytest.raises(NotPartitionedError):
            require_list_partitioned(
                partitioned_db.connection(), "observation"
            )

    def test_require_list_rejects_range(self, partitioned_db: orm.Session):
        _sql(
            partitioned_db,
            "CREATE TABLE ranged (k int, v int, PRIMARY KEY (k)) "
            "PARTITION BY RANGE (k)",
        )
        conn = partitioned_db.connection()
        assert partition_strategy(conn, "ranged").strategy == "range"
        with pytest.raises(UnsupportedPartitionStrategyError):
            require_list_partitioned(conn, "ranged")

    def test_multi_column_key_column_property_refuses(
        self, partitioned_db: orm.Session
    ):
        _sql(
            partitioned_db,
            "CREATE TABLE two_key (a int, b int, PRIMARY KEY (a, b)) "
            "PARTITION BY RANGE (a, b)",
        )
        strategy = partition_strategy(partitioned_db.connection(), "two_key")
        assert strategy.key_columns == ("a", "b")
        with pytest.raises(UnsupportedPartitionStrategyError):
            strategy.key_column

    def test_expression_key_refused(self, partitioned_db: orm.Session):
        _sql(
            partitioned_db,
            "CREATE TABLE expr_key (k int, v int) PARTITION BY LIST ((k % 4))",
        )
        with pytest.raises(UnsupportedPartitionStrategyError):
            partition_strategy(partitioned_db.connection(), "expr_key")


class TestListTablePartitions:
    def test_empty(self, partitioned_db: orm.Session):
        assert (
            list_table_partitions(partitioned_db.connection(), "dataset") == []
        )

    def test_missing_raises(self, partitioned_db: orm.Session):
        with pytest.raises(RelationNotFoundError):
            list_table_partitions(partitioned_db.connection(), "no_such")

    def test_lists_bounds_and_sizes(self, partitioned_db: orm.Session):
        _create_partition(partitioned_db, "dataset", 5)
        _create_partition(partitioned_db, "dataset", 7)
        infos = list_table_partitions(partitioned_db.connection(), "dataset")

        assert [i.name for i in infos] == ["dataset_obs_5", "dataset_obs_7"]
        five = infos[0]
        assert five.parent == "dataset"
        assert five.schema == "public"
        assert five.qualified_name == "public.dataset_obs_5"
        assert five.bound == "FOR VALUES IN (5)"
        assert five.list_values == (5,)
        assert not five.is_default
        assert not five.detach_pending
        assert five.heap_bytes >= 0
        assert five.index_bytes >= 0
        assert five.total_bytes >= five.heap_bytes + five.index_bytes
        assert five.toast_bytes >= 0

    def test_accepts_table_and_mapped_class(self, partitioned_db: orm.Session):
        _create_partition(partitioned_db, "dataset", 5)
        conn = partitioned_db.connection()
        assert len(list_table_partitions(conn, DataSet.__table__)) == 1
        assert len(list_table_partitions(conn, DataSet)) == 1

    def test_default_partition_reported(self, v2_db: orm.Session):
        infos = list_table_partitions(v2_db.connection(), "dataset")
        assert [i.name for i in infos] == ["dataset_default"]
        assert infos[0].is_default
        assert infos[0].bound == "DEFAULT"
        assert infos[0].list_values == ()


class TestFindPartition:
    def test_finds_by_value(self, partitioned_db: orm.Session):
        _create_partition(partitioned_db, "dataset", 5)
        _create_partition(partitioned_db, "dataset", 7)
        conn = partitioned_db.connection()
        assert find_partition_for_value(conn, "dataset", 7).name == (
            "dataset_obs_7"
        )
        assert find_partition_for_value(conn, "dataset", 99) is None

    def test_default_is_never_the_answer(self, v2_db: orm.Session):
        """A value routed to DEFAULT has no partition *for* it."""
        conn = v2_db.connection()
        assert find_partition_for_value(conn, "dataset", 99) is None
        assert default_partition_of(conn, "dataset").name == "dataset_default"

    def test_no_default(self, partitioned_db: orm.Session):
        assert (
            default_partition_of(partitioned_db.connection(), "dataset")
            is None
        )


class TestCheckAttachable:
    def test_fully_prepared_candidate_is_ok(self, partitioned_db: orm.Session):
        """The target state: ATTACH would be a pure catalog operation."""
        name = _staged_candidate(partitioned_db, 5)
        report = check_attachable(
            partitioned_db.connection(), "dataset", name, 5
        )
        assert report.ok, (report.blocking, report.expensive)
        assert report.candidate_kind == "table"
        assert report.key_column == "observation_id"
        report.raise_for_status()

    def test_prepared_candidate_actually_attaches(
        self, partitioned_db: orm.Session
    ):
        """The pre-flight's verdict must agree with PostgreSQL."""
        name = _staged_candidate(partitioned_db, 5)
        check_attachable(
            partitioned_db.connection(), "dataset", name, 5
        ).raise_for_status()
        _sql(
            partitioned_db,
            f"ALTER TABLE dataset ATTACH PARTITION {name} FOR VALUES IN (5)",
        )
        found = find_partition_for_value(
            partitioned_db.connection(), "dataset", 5
        )
        assert found is not None and found.name == name

    def test_missing_check_is_expensive_not_blocking(
        self, partitioned_db: orm.Session
    ):
        name = _staged_candidate(partitioned_db, 5, check=False)
        report = check_attachable(
            partitioned_db.connection(), "dataset", name, 5
        )
        assert not report.has_valid_partition_check
        assert not report.blocking
        assert any("CHECK" in e for e in report.expensive)
        with pytest.raises(NotAttachableError):
            report.raise_for_status()
        report.raise_for_status(allow_expensive=True)

    def test_not_valid_check_does_not_count(self, partitioned_db: orm.Session):
        """Only a validated CHECK lets PostgreSQL skip the scan."""
        name = _staged_candidate(partitioned_db, 5, check=False)
        _sql(
            partitioned_db,
            f"ALTER TABLE {name} ADD CONSTRAINT {name}_partcheck "
            "CHECK (observation_id = 5) NOT VALID",
        )
        report = check_attachable(
            partitioned_db.connection(), "dataset", name, 5
        )
        assert not report.has_valid_partition_check

    def test_check_for_wrong_value_does_not_count(
        self, partitioned_db: orm.Session
    ):
        name = _staged_candidate(partitioned_db, 5)
        report = check_attachable(
            partitioned_db.connection(), "dataset", name, 6
        )
        assert not report.has_valid_partition_check

    def test_missing_column_blocks(self, partitioned_db: orm.Session):
        name = _staged_candidate(partitioned_db, 5)
        _sql(partitioned_db, f"ALTER TABLE {name} DROP COLUMN errors")
        report = check_attachable(
            partitioned_db.connection(), "dataset", name, 5
        )
        assert report.column_mismatches == ("errors missing from candidate",)
        assert report.blocking

    def test_extra_column_blocks(self, partitioned_db: orm.Session):
        name = _staged_candidate(partitioned_db, 5)
        _sql(partitioned_db, f"ALTER TABLE {name} ADD COLUMN extra int")
        report = check_attachable(
            partitioned_db.connection(), "dataset", name, 5
        )
        assert report.column_mismatches == (
            "extra on candidate but not on parent",
        )

    def test_type_mismatch_blocks(self, partitioned_db: orm.Session):
        name = _staged_candidate(partitioned_db, 5, primary_key=False)
        _sql(
            partitioned_db,
            f"ALTER TABLE {name} ALTER COLUMN target_id TYPE integer",
        )
        report = check_attachable(
            partitioned_db.connection(), "dataset", name, 5
        )
        assert any(
            "target_id is bigint" in m for m in report.column_mismatches
        )

    def test_nullable_where_parent_not_null_blocks(
        self, partitioned_db: orm.Session
    ):
        name = _staged_candidate(partitioned_db, 5)
        _sql(
            partitioned_db,
            f"ALTER TABLE {name} ALTER COLUMN values DROP NOT NULL",
        )
        report = check_attachable(
            partitioned_db.connection(), "dataset", name, 5
        )
        assert any(
            "nullable on candidate" in m for m in report.column_mismatches
        )

    def test_missing_index_is_expensive(self, partitioned_db: orm.Session):
        name = _staged_candidate(partitioned_db, 5, target_index=False)
        report = check_attachable(
            partitioned_db.connection(), "dataset", name, 5
        )
        assert report.missing_indexes == ("ix_dataset_target_id",)
        assert not report.blocking

    def test_bare_unique_index_instead_of_pk_is_expensive(
        self, partitioned_db: orm.Session
    ):
        """PostgreSQL will not adopt a constraint-less match for a PK index;
        it rebuilds instead, under ACCESS EXCLUSIVE."""
        name = _staged_candidate(partitioned_db, 5, primary_key=False)
        _sql(
            partitioned_db,
            f"CREATE UNIQUE INDEX {name}_uq ON {name} "
            "(observation_id, target_id, photometric_method_id, "
            "processing_method_id)",
        )
        report = check_attachable(
            partitioned_db.connection(), "dataset", name, 5
        )
        assert report.missing_indexes == ()
        assert report.unbacked_constraint_indexes == (f"{name}_uq",)
        assert not report.ok

    def test_missing_foreign_keys_are_expensive(
        self, partitioned_db: orm.Session
    ):
        name = _staged_candidate(partitioned_db, 5, foreign_keys=False)
        report = check_attachable(
            partitioned_db.connection(), "dataset", name, 5
        )
        assert len(report.missing_foreign_keys) == 4
        assert not report.blocking
        assert not report.ok

    def test_default_partition_is_expensive(self, v2_db: orm.Session):
        name = _staged_candidate(v2_db, 5)
        report = check_attachable(v2_db.connection(), "dataset", name, 5)
        assert report.default_partition == "dataset_default"
        assert not report.default_partition_conflicts
        assert any("default partition" in e for e in report.expensive)
        assert not report.blocking

    def test_conflicting_default_row_blocks(self, v2_db: orm.Session):
        """The failure PostgreSQL reports as 'updated partition constraint
        for default partition ... would be violated by some row'."""
        _sql(
            v2_db,
            "INSERT INTO instrument (id, name, properties) "
            "VALUES (gen_random_uuid(), 'i', '{}')",
        )
        _sql(
            v2_db,
            "INSERT INTO observation (id, type, cadence_reference, "
            "instrument_id) "
            "SELECT 5, 'observation', '{1}', id FROM instrument",
        )
        _sql(
            v2_db,
            "INSERT INTO mission (id, name, description, time_epoch, "
            "time_epoch_scale, time_epoch_format, time_format_name) VALUES "
            "(gen_random_uuid(), 'm', 'd', 0, 'tdb', 'jd', 't')",
        )
        _sql(
            v2_db,
            "INSERT INTO mission_catalog (id, name, description, "
            "host_mission_id) SELECT 1, 'c', 'd', id FROM mission",
        )
        _sql(
            v2_db, "INSERT INTO target (id, catalog_id, name) VALUES (1, 1, 1)"
        )
        _sql(v2_db, "INSERT INTO dataset VALUES (5, 1, 0, 0, '{1.0}', NULL)")

        name = _staged_candidate(v2_db, 5)
        report = check_attachable(v2_db.connection(), "dataset", name, 5)
        assert report.default_partition_conflicts
        assert any("holds rows" in b for b in report.blocking)
        with pytest.raises(NotAttachableError):
            report.raise_for_status(allow_expensive=True)

    def test_absent_candidate_is_reported_not_raised(
        self, partitioned_db: orm.Session
    ):
        report = check_attachable(
            partitioned_db.connection(), "dataset", "not_yet_created", 5
        )
        assert report.candidate_kind is None
        assert any("absent" in b for b in report.blocking)

    def test_partitioned_candidate_blocks(self, partitioned_db: orm.Session):
        report = check_attachable(
            partitioned_db.connection(), "dataset", "datasethierarchy", 5
        )
        assert report.candidate_kind == "partitioned table"
        assert report.blocking

    def test_missing_parent_raises(self, partitioned_db: orm.Session):
        with pytest.raises(RelationNotFoundError):
            check_attachable(partitioned_db.connection(), "no_such", "x", 5)

    def test_unpartitioned_parent_raises(self, partitioned_db: orm.Session):
        with pytest.raises(NotPartitionedError):
            check_attachable(
                partitioned_db.connection(), "observation", "x", 5
            )

    def test_error_message_lists_every_finding(
        self, partitioned_db: orm.Session
    ):
        name = _staged_candidate(
            partitioned_db, 5, check=False, target_index=False
        )
        _sql(partitioned_db, f"ALTER TABLE {name} DROP COLUMN errors")
        with pytest.raises(NotAttachableError) as excinfo:
            check_attachable(
                partitioned_db.connection(), "dataset", name, 5
            ).raise_for_status()
        message = str(excinfo.value)
        assert "errors missing" in message
        assert "ix_dataset_target_id" in message
        assert "CHECK" in message
