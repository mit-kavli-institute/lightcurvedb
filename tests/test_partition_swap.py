"""Tests for the atomic multi-table partition swap.

The swap is the riskiest statement sequence in the package, so most of
these are end-to-end: real partitions, real rows, a real transaction.
Row counts are tiny -- correctness here is about ordering, atomicity and
what survives, none of which scale with the data.
"""

import pytest
import sqlalchemy as sa
from sqlalchemy import orm

from lightcurvedb.core.partitions import (
    NotAttachableError,
    PartitionError,
    SwapRaceError,
    analyze_relation,
    build_partition_indexes,
    detach_partition,
    drop_retired,
    ensure_partition,
    ensure_staging_table,
    find_partition_for_value,
    is_lock_not_available,
    mirror_outbound_foreign_keys,
    next_revision,
    plan_swap,
    preflight,
    revisions_of,
    rollback_swap,
    swap,
)
from lightcurvedb.models import DataSet, Observation, Target

pytestmark = pytest.mark.partitioning

PAIR = ("dataset", "datasethierarchy")


@pytest.fixture
def orm_session(partitioned_db):
    return partitioned_db


def _dataset_row(
    session: orm.Session, relation: str, key: int, target: int, value: float
) -> None:
    session.execute(
        sa.text(
            f"INSERT INTO {relation} (observation_id, target_id, "
            "photometric_method_id, processing_method_id, values) "
            "VALUES (:key, :target, 0, 0, ARRAY[:value]::float8[])"
        ),
        {"key": key, "target": target, "value": value},
    )


def _hierarchy_row(
    session: orm.Session, relation: str, key: int, source: int, child: int
) -> None:
    session.execute(
        sa.text(
            f"INSERT INTO {relation} VALUES "
            "(:key, :source, 0, 0, :key, :child, 0, 0)"
        ),
        {"key": key, "source": source, "child": child},
    )


def _extra_target(session: orm.Session, target: Target, offset: int) -> int:
    """Another target in the same catalog, so lineage has two ends."""
    other = Target(catalog_id=target.catalog_id, name=target.name + offset)
    session.add(other)
    session.flush()
    return other.id


def _provision_live(
    session: orm.Session, key: int, targets: tuple[int, int], value: float
) -> None:
    """A live revision 0 for both parents, holding ``value``."""
    conn = session.connection()
    dataset = ensure_partition(conn, "dataset", key).table
    hierarchy = ensure_partition(conn, "datasethierarchy", key).table
    for target in targets:
        _dataset_row(session, dataset, key, target, value)
    _hierarchy_row(session, hierarchy, key, *targets)


def _stage(
    session: orm.Session,
    key: int,
    revision: int,
    targets: tuple[int, int],
    value: float,
) -> tuple[str, str]:
    """A fully prepared revision, ready to attach."""
    conn = session.connection()
    dataset = ensure_staging_table(conn, "dataset", key, revision).table
    hierarchy = ensure_staging_table(
        conn, "datasethierarchy", key, revision
    ).table
    for target in targets:
        _dataset_row(session, dataset, key, target, value)
    _hierarchy_row(session, hierarchy, key, *targets)

    build_partition_indexes(conn, "dataset", dataset)
    build_partition_indexes(conn, "datasethierarchy", hierarchy)
    mirror_outbound_foreign_keys(conn, "dataset", dataset)
    analyze_relation(conn, dataset)
    analyze_relation(conn, hierarchy)
    return dataset, hierarchy


def _first_value(session: orm.Session, relation: str, key: int) -> float:
    return session.execute(
        sa.text(
            f"SELECT DISTINCT values[1] FROM {relation} "
            "WHERE observation_id = :key"
        ),
        {"key": key},
    ).scalar()


def _is_attached(session: orm.Session, relation: str) -> bool:
    return bool(
        session.execute(
            sa.text(
                "SELECT relispartition FROM pg_class WHERE relname = :name"
            ),
            {"name": relation},
        ).scalar()
    )


class TestPlanning:
    def test_the_referencing_side_is_ordered_last(
        self, partitioned_db: orm.Session, sample_observation: Observation
    ):
        """Derived from the catalog, not from the order given."""
        conn = partitioned_db.connection()
        plan = plan_swap(
            conn,
            [
                ("datasethierarchy", "datasethierarchy_obs_1_v1"),
                ("dataset", "dataset_obs_1_v1"),
            ],
            sample_observation.id,
        )
        assert plan.parents == PAIR

    def test_it_records_what_is_currently_live(
        self,
        partitioned_db: orm.Session,
        sample_observation: Observation,
        sample_target: Target,
    ):
        key = sample_observation.id
        targets = (
            sample_target.id,
            _extra_target(partitioned_db, sample_target, 1),
        )
        _provision_live(partitioned_db, key, targets, 1.0)
        conn = partitioned_db.connection()

        plan = plan_swap(
            conn,
            [(name, f"{name}_obs_{key}_v1") for name in PAIR],
            key,
        )
        assert [pair.retiring for pair in plan.pairs] == [
            f"dataset_obs_{key}",
            f"datasethierarchy_obs_{key}",
        ]

    def test_referenced_tables_are_collected_for_locking(
        self, partitioned_db: orm.Session, sample_observation: Observation
    ):
        plan = plan_swap(
            partitioned_db.connection(),
            [(name, f"{name}_v1") for name in PAIR],
            sample_observation.id,
        )
        assert plan.referenced == (
            "observation",
            "photometric_source",
            "processing_method",
            "target",
        )


class TestPreflight:
    def test_a_prepared_pair_has_nothing_blocking(
        self,
        partitioned_db: orm.Session,
        sample_observation: Observation,
        sample_target: Target,
    ):
        key = sample_observation.id
        targets = (
            sample_target.id,
            _extra_target(partitioned_db, sample_target, 1),
        )
        _provision_live(partitioned_db, key, targets, 1.0)
        staged = _stage(partitioned_db, key, 1, targets, 9.9)
        conn = partitioned_db.connection()

        plan = plan_swap(conn, list(zip(PAIR, staged)), key)
        report = preflight(conn, plan)

        assert report.blocking == ()
        report.raise_for_status(allow_expensive=True)

    def test_the_hierarchy_attach_is_reported_as_expensive(
        self,
        partitioned_db: orm.Session,
        sample_observation: Observation,
        sample_target: Target,
    ):
        """Its foreign keys cannot be pre-validated -- section 8.1."""
        key = sample_observation.id
        targets = (
            sample_target.id,
            _extra_target(partitioned_db, sample_target, 1),
        )
        _provision_live(partitioned_db, key, targets, 1.0)
        staged = _stage(partitioned_db, key, 1, targets, 9.9)
        conn = partitioned_db.connection()

        report = preflight(conn, plan_swap(conn, list(zip(PAIR, staged)), key))

        assert any(
            "datasethierarchy" in finding and "foreign key" in finding
            for finding in report.expensive
        )
        assert not report.ok
        with pytest.raises(NotAttachableError, match="would not be clean"):
            report.raise_for_status()

    def test_orphaned_lineage_is_found_before_any_lock(
        self,
        partitioned_db: orm.Session,
        sample_observation: Observation,
        sample_target: Target,
    ):
        """A hierarchy row whose dataset row is not in the staged set."""
        key = sample_observation.id
        targets = (
            sample_target.id,
            _extra_target(partitioned_db, sample_target, 1),
        )
        _provision_live(partitioned_db, key, targets, 1.0)
        dataset, hierarchy = _stage(partitioned_db, key, 1, targets, 9.9)
        stranger = _extra_target(partitioned_db, sample_target, 2)
        _hierarchy_row(partitioned_db, hierarchy, key, targets[0], stranger)
        conn = partitioned_db.connection()

        report = preflight(
            conn, plan_swap(conn, list(zip(PAIR, (dataset, hierarchy))), key)
        )

        assert len(report.orphans) == 1
        assert "1 row(s)" in report.orphans[0]
        assert not report.ok


class TestSwap:
    def test_it_promotes_the_new_and_retires_the_old(
        self,
        partitioned_db: orm.Session,
        sample_observation: Observation,
        sample_target: Target,
    ):
        key = sample_observation.id
        targets = (
            sample_target.id,
            _extra_target(partitioned_db, sample_target, 1),
        )
        _provision_live(partitioned_db, key, targets, 1.0)
        staged = _stage(partitioned_db, key, 1, targets, 9.9)
        conn = partitioned_db.connection()

        result = swap(conn, plan_swap(conn, list(zip(PAIR, staged)), key))
        partitioned_db.commit()

        assert _first_value(partitioned_db, "dataset", key) == 9.9
        assert result.promoted == staged
        assert result.retired == (
            f"dataset_obs_{key}",
            f"datasethierarchy_obs_{key}",
        )

    def test_the_retired_data_is_still_there_and_readable(
        self,
        partitioned_db: orm.Session,
        sample_observation: Observation,
        sample_target: Target,
    ):
        """The coexistence requirement: old and new both on disk."""
        key = sample_observation.id
        targets = (
            sample_target.id,
            _extra_target(partitioned_db, sample_target, 1),
        )
        _provision_live(partitioned_db, key, targets, 1.0)
        staged = _stage(partitioned_db, key, 1, targets, 9.9)
        conn = partitioned_db.connection()

        result = swap(conn, plan_swap(conn, list(zip(PAIR, staged)), key))
        partitioned_db.commit()

        retired = result.retired[0]
        assert not _is_attached(partitioned_db, retired)
        assert _first_value(partitioned_db, retired, key) == 1.0

    def test_ordinary_orm_queries_see_only_the_live_revision(
        self,
        partitioned_db: orm.Session,
        sample_observation: Observation,
        sample_target: Target,
    ):
        """Consumers need no knowledge of any of this."""
        key = sample_observation.id
        targets = (
            sample_target.id,
            _extra_target(partitioned_db, sample_target, 1),
        )
        _provision_live(partitioned_db, key, targets, 1.0)
        staged = _stage(partitioned_db, key, 1, targets, 9.9)
        partitioned_db.commit()

        before = partitioned_db.scalars(
            sa.select(DataSet).where(DataSet.observation_id == key)
        ).all()
        assert {row.values[0] for row in before} == {1.0}

        conn = partitioned_db.connection()
        swap(conn, plan_swap(conn, list(zip(PAIR, staged)), key))
        partitioned_db.commit()
        partitioned_db.expunge_all()

        after = partitioned_db.scalars(
            sa.select(DataSet).where(DataSet.observation_id == key)
        ).all()
        assert {row.values[0] for row in after} == {9.9}
        assert len(after) == len(before)

    def test_a_later_campaign_supersedes_without_touching_the_first(
        self,
        partitioned_db: orm.Session,
        sample_observation: Observation,
        sample_target: Target,
    ):
        """Replacement recurs: v0 retired, v1 live, then v1 retired too."""
        key = sample_observation.id
        targets = (
            sample_target.id,
            _extra_target(partitioned_db, sample_target, 1),
        )
        _provision_live(partitioned_db, key, targets, 1.0)

        first = _stage(partitioned_db, key, 1, targets, 9.9)
        conn = partitioned_db.connection()
        swap(conn, plan_swap(conn, list(zip(PAIR, first)), key))
        partitioned_db.commit()

        conn = partitioned_db.connection()
        assert next_revision(conn, "dataset", key) == 2
        second = _stage(partitioned_db, key, 2, targets, 0.5)
        result = swap(conn, plan_swap(conn, list(zip(PAIR, second)), key))
        partitioned_db.commit()

        assert _first_value(partitioned_db, "dataset", key) == 0.5
        assert result.retired[0] == first[0]
        # Every revision is still on disk, and each still holds its own
        # data: nothing is dropped until somebody says so.
        assert set(
            revisions_of(partitioned_db.connection(), "dataset", key)
        ) == {
            0,
            1,
            2,
        }
        assert _first_value(partitioned_db, f"dataset_obs_{key}", key) == 1.0
        assert _first_value(partitioned_db, first[0], key) == 9.9

    def test_retired_relations_get_their_bound_check_back(
        self,
        partitioned_db: orm.Session,
        sample_observation: Observation,
        sample_target: Target,
    ):
        """Without it a rollback would re-validate every row."""
        key = sample_observation.id
        targets = (
            sample_target.id,
            _extra_target(partitioned_db, sample_target, 1),
        )
        _provision_live(partitioned_db, key, targets, 1.0)
        staged = _stage(partitioned_db, key, 1, targets, 9.9)
        conn = partitioned_db.connection()

        result = swap(conn, plan_swap(conn, list(zip(PAIR, staged)), key))

        for retired in result.retired:
            definition = partitioned_db.execute(
                sa.text(
                    "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
                    "WHERE conname = :name"
                ),
                {"name": f"{retired}_partcheck"},
            ).scalar()
            assert definition is not None

    def test_rolling_back_the_transaction_undoes_everything(
        self,
        partitioned_db: orm.Session,
        sample_observation: Observation,
        sample_target: Target,
    ):
        """Before COMMIT, ROLLBACK is the entire undo."""
        key = sample_observation.id
        targets = (
            sample_target.id,
            _extra_target(partitioned_db, sample_target, 1),
        )
        _provision_live(partitioned_db, key, targets, 1.0)
        staged = _stage(partitioned_db, key, 1, targets, 9.9)
        partitioned_db.commit()
        conn = partitioned_db.connection()

        swap(conn, plan_swap(conn, list(zip(PAIR, staged)), key))
        partitioned_db.rollback()

        live = find_partition_for_value(
            partitioned_db.connection(), "dataset", key
        )
        assert live is not None and live.name == f"dataset_obs_{key}"
        assert _first_value(partitioned_db, "dataset", key) == 1.0

    def test_a_failed_step_leaves_nothing_moved(
        self,
        partitioned_db: orm.Session,
        sample_observation: Observation,
        sample_target: Target,
    ):
        """An orphan makes the hierarchy attach fail, so nothing lands."""
        key = sample_observation.id
        targets = (
            sample_target.id,
            _extra_target(partitioned_db, sample_target, 1),
        )
        _provision_live(partitioned_db, key, targets, 1.0)
        dataset, hierarchy = _stage(partitioned_db, key, 1, targets, 9.9)
        partitioned_db.execute(
            sa.text(
                f"INSERT INTO {hierarchy} VALUES "
                "(:key, :source, 0, 0, :key, 987654321, 0, 0)"
            ),
            {"key": key, "source": targets[0]},
        )
        partitioned_db.commit()
        conn = partitioned_db.connection()

        with pytest.raises(sa.exc.IntegrityError):
            swap(
                conn,
                plan_swap(conn, list(zip(PAIR, (dataset, hierarchy))), key),
            )
        partitioned_db.rollback()

        live = find_partition_for_value(
            partitioned_db.connection(), "dataset", key
        )
        assert live is not None and live.name == f"dataset_obs_{key}"
        assert _first_value(partitioned_db, "dataset", key) == 1.0
        assert not _is_attached(partitioned_db, dataset)

    def test_it_refuses_when_the_live_partition_moved_under_it(
        self,
        partitioned_db: orm.Session,
        sample_observation: Observation,
        sample_target: Target,
    ):
        key = sample_observation.id
        targets = (
            sample_target.id,
            _extra_target(partitioned_db, sample_target, 1),
        )
        _provision_live(partitioned_db, key, targets, 1.0)
        staged = _stage(partitioned_db, key, 1, targets, 9.9)
        conn = partitioned_db.connection()
        plan = plan_swap(conn, list(zip(PAIR, staged)), key)

        detach_partition(
            conn, "datasethierarchy", f"datasethierarchy_obs_{key}"
        )
        detach_partition(conn, "dataset", f"dataset_obs_{key}")

        with pytest.raises(SwapRaceError, match="Re-plan"):
            swap(conn, plan)

    def test_it_extends_to_a_third_table_without_code_changes(
        self,
        partitioned_db: orm.Session,
        sample_observation: Observation,
        sample_target: Target,
    ):
        """``target_specific_time`` joins by being named in the plan."""
        key = sample_observation.id
        targets = (
            sample_target.id,
            _extra_target(partitioned_db, sample_target, 1),
        )
        _provision_live(partitioned_db, key, targets, 1.0)
        conn = partitioned_db.connection()
        live_times = ensure_partition(conn, "target_specific_time", key).table
        partitioned_db.execute(
            sa.text(
                f"INSERT INTO {live_times} VALUES "
                "(:key, :target, ARRAY[1.0]::float8[])"
            ),
            {"key": key, "target": targets[0]},
        )

        staged = _stage(partitioned_db, key, 1, targets, 9.9)
        times = ensure_staging_table(
            conn, "target_specific_time", key, 1
        ).table
        partitioned_db.execute(
            sa.text(
                f"INSERT INTO {times} VALUES "
                "(:key, :target, ARRAY[2.0]::float8[])"
            ),
            {"key": key, "target": targets[0]},
        )
        build_partition_indexes(conn, "target_specific_time", times)
        mirror_outbound_foreign_keys(conn, "target_specific_time", times)

        plan = plan_swap(
            conn,
            list(zip(PAIR, staged)) + [("target_specific_time", times)],
            key,
        )
        result = swap(conn, plan)
        partitioned_db.commit()

        assert len(result.steps) == 3
        assert (
            partitioned_db.execute(
                sa.text(
                    "SELECT barycentric_julian_dates[1] FROM "
                    "target_specific_time WHERE observation_id = :key"
                ),
                {"key": key},
            ).scalar()
            == 2.0
        )


class TestRollbackSwap:
    def test_the_mirror_swap_restores_the_old_data(
        self,
        partitioned_db: orm.Session,
        sample_observation: Observation,
        sample_target: Target,
    ):
        key = sample_observation.id
        targets = (
            sample_target.id,
            _extra_target(partitioned_db, sample_target, 1),
        )
        _provision_live(partitioned_db, key, targets, 1.0)
        staged = _stage(partitioned_db, key, 1, targets, 9.9)
        conn = partitioned_db.connection()
        result = swap(conn, plan_swap(conn, list(zip(PAIR, staged)), key))
        partitioned_db.commit()

        reversal = rollback_swap(partitioned_db.connection(), result)
        partitioned_db.commit()

        assert _first_value(partitioned_db, "dataset", key) == 1.0
        assert reversal.retired == staged

    def test_there_is_nothing_to_restore_for_a_first_promotion(
        self,
        partitioned_db: orm.Session,
        sample_observation: Observation,
        sample_target: Target,
    ):
        key = sample_observation.id
        targets = (
            sample_target.id,
            _extra_target(partitioned_db, sample_target, 1),
        )
        staged = _stage(partitioned_db, key, 1, targets, 9.9)
        conn = partitioned_db.connection()
        result = swap(conn, plan_swap(conn, list(zip(PAIR, staged)), key))

        with pytest.raises(PartitionError, match="empty slot"):
            rollback_swap(conn, result)


class TestDropRetired:
    def test_it_lists_without_dropping_by_default(
        self,
        partitioned_db: orm.Session,
        sample_observation: Observation,
        sample_target: Target,
    ):
        key = sample_observation.id
        targets = (
            sample_target.id,
            _extra_target(partitioned_db, sample_target, 1),
        )
        _provision_live(partitioned_db, key, targets, 1.0)
        staged = _stage(partitioned_db, key, 1, targets, 9.9)
        conn = partitioned_db.connection()
        result = swap(conn, plan_swap(conn, list(zip(PAIR, staged)), key))

        listed = drop_retired(conn, result.retired)

        assert listed == result.retired
        assert _first_value(partitioned_db, result.retired[0], key) == 1.0

    def test_it_drops_when_told_to(
        self,
        partitioned_db: orm.Session,
        sample_observation: Observation,
        sample_target: Target,
    ):
        key = sample_observation.id
        targets = (
            sample_target.id,
            _extra_target(partitioned_db, sample_target, 1),
        )
        _provision_live(partitioned_db, key, targets, 1.0)
        staged = _stage(partitioned_db, key, 1, targets, 9.9)
        conn = partitioned_db.connection()
        result = swap(conn, plan_swap(conn, list(zip(PAIR, staged)), key))

        dropped = drop_retired(conn, result.retired, dry_run=False)

        assert dropped == result.retired
        assert drop_retired(conn, result.retired) == ()

    def test_it_refuses_a_relation_that_is_still_attached(
        self, partitioned_db: orm.Session, sample_observation: Observation
    ):
        conn = partitioned_db.connection()
        key = sample_observation.id
        live = ensure_partition(conn, "dataset", key).table

        with pytest.raises(PartitionError, match="live data"):
            drop_retired(conn, [live], dry_run=False)


@pytest.mark.slow
class TestLockBehaviour:
    def test_a_held_lock_makes_the_swap_fail_cleanly(
        self,
        partitioned_db: orm.Session,
        database_engine: sa.Engine,
        sample_observation: Observation,
        sample_target: Target,
    ):
        """lock_timeout bounds the wait; 55P03 is the clean outcome."""
        key = sample_observation.id
        targets = (
            sample_target.id,
            _extra_target(partitioned_db, sample_target, 1),
        )
        _provision_live(partitioned_db, key, targets, 1.0)
        staged = _stage(partitioned_db, key, 1, targets, 9.9)
        partitioned_db.commit()

        reader = database_engine.connect()
        try:
            reader.execute(sa.text("SELECT count(*) FROM dataset"))
            conn = partitioned_db.connection()
            plan = plan_swap(
                conn, list(zip(PAIR, staged)), key, lock_timeout="100ms"
            )
            assert plan.lock_timeout == "100ms"

            with pytest.raises(sa.exc.OperationalError) as caught:
                swap(conn, plan)
            assert is_lock_not_available(caught.value)
        finally:
            reader.close()
            partitioned_db.rollback()

        live = find_partition_for_value(
            partitioned_db.connection(), "dataset", key
        )
        assert live is not None and live.name == f"dataset_obs_{key}"
