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
    has_bound_check,
    is_lock_not_available,
    mirror_outbound_foreign_keys,
    next_revision,
    plan_swap,
    preflight,
    prepare_retirement,
    revisions_of,
    rollback_swap,
    swap,
)
from lightcurvedb.models import DataSet, Observation, Target

pytestmark = [
    pytest.mark.partitioning,
    pytest.mark.usefixtures("dataset_link"),
]

PAIR = ("dataset", "datasetlink")


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


def _link_row(
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
    """Another target in the same catalog, so a link has two ends."""
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
    link = ensure_partition(conn, "datasetlink", key).table
    for target in targets:
        _dataset_row(session, dataset, key, target, value)
    _link_row(session, link, key, *targets)


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
    link = ensure_staging_table(conn, "datasetlink", key, revision).table
    for target in targets:
        _dataset_row(session, dataset, key, target, value)
    _link_row(session, link, key, *targets)

    build_partition_indexes(conn, "dataset", dataset)
    build_partition_indexes(conn, "datasetlink", link)
    mirror_outbound_foreign_keys(conn, "dataset", dataset)
    analyze_relation(conn, dataset)
    analyze_relation(conn, link)
    return dataset, link


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
                ("datasetlink", "datasetlink_obs_1_v1"),
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
            f"datasetlink_obs_{key}",
        ]

    def test_it_refuses_the_same_parent_twice(
        self, partitioned_db: orm.Session, sample_observation: Observation
    ):
        """Keyed by parent, a duplicate would silently drop a promotion."""
        conn = partitioned_db.connection()
        with pytest.raises(ValueError, match="appears twice"):
            plan_swap(
                conn,
                [("dataset", "dataset_v1"), ("dataset", "dataset_v2")],
                sample_observation.id,
            )

    def test_it_refuses_to_narrow_a_multi_value_bound(
        self, partitioned_db: orm.Session, sample_observation: Observation
    ):
        """Replacing IN (5, 6) for 5 alone would strand 6."""
        key = sample_observation.id
        partitioned_db.execute(
            sa.text(
                "CREATE TABLE dataset_obs_pair PARTITION OF dataset "
                f"FOR VALUES IN ({key}, {key + 1})"
            )
        )
        conn = partitioned_db.connection()

        with pytest.raises(PartitionError, match="Split the bound"):
            plan_swap(conn, [("dataset", "dataset_obs_9_v1")], key)

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

    def test_the_link_attach_is_reported_as_expensive(
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
            "datasetlink" in finding and "foreign key" in finding
            for finding in report.expensive
        )
        assert not report.ok
        with pytest.raises(NotAttachableError, match="would not be clean"):
            report.raise_for_status()

    def test_orphaned_links_are_found_before_any_lock(
        self,
        partitioned_db: orm.Session,
        sample_observation: Observation,
        sample_target: Target,
    ):
        """A link row whose dataset row is not in the staged set."""
        key = sample_observation.id
        targets = (
            sample_target.id,
            _extra_target(partitioned_db, sample_target, 1),
        )
        _provision_live(partitioned_db, key, targets, 1.0)
        dataset, link = _stage(partitioned_db, key, 1, targets, 9.9)
        stranger = _extra_target(partitioned_db, sample_target, 2)
        _link_row(partitioned_db, link, key, targets[0], stranger)
        conn = partitioned_db.connection()

        report = preflight(
            conn, plan_swap(conn, list(zip(PAIR, (dataset, link))), key)
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
            f"datasetlink_obs_{key}",
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

    def test_retired_relations_still_carry_their_bound_check(
        self,
        partitioned_db: orm.Session,
        sample_observation: Observation,
        sample_target: Target,
    ):
        """Kept from provisioning, not added under the lock."""
        key = sample_observation.id
        targets = (
            sample_target.id,
            _extra_target(partitioned_db, sample_target, 1),
        )
        _provision_live(partitioned_db, key, targets, 1.0)
        staged = _stage(partitioned_db, key, 1, targets, 9.9)
        conn = partitioned_db.connection()

        result = swap(conn, plan_swap(conn, list(zip(PAIR, staged)), key))

        for parent, retired in zip(PAIR, result.retired):
            key_column = (
                "observation_id"
                if parent == "dataset"
                else "source_observation_id"
            )
            assert has_bound_check(conn, retired, key_column, key)

    def test_the_swap_adds_no_constraint_of_its_own(
        self,
        partitioned_db: orm.Session,
        sample_observation: Observation,
        sample_target: Target,
    ):
        """Validating a CHECK is a scan, and every parent is locked.

        A hand-made partition carries no bound CHECK. The swap must not
        quietly add one: that would read every row while holding ACCESS
        EXCLUSIVE on every parent, which is the outage this design
        exists to avoid. Preflight says so instead.
        """
        key = sample_observation.id
        targets = (
            sample_target.id,
            _extra_target(partitioned_db, sample_target, 1),
        )
        partitioned_db.execute(
            sa.text(
                f"CREATE TABLE dataset_obs_{key} PARTITION OF dataset "
                f"FOR VALUES IN ({key})"
            )
        )
        _dataset_row(
            partitioned_db, f"dataset_obs_{key}", key, targets[0], 1.0
        )
        conn = partitioned_db.connection()
        dataset = ensure_staging_table(conn, "dataset", key, 1).table
        _dataset_row(partitioned_db, dataset, key, targets[0], 9.9)
        build_partition_indexes(conn, "dataset", dataset)
        mirror_outbound_foreign_keys(conn, "dataset", dataset)

        plan = plan_swap(conn, [("dataset", dataset)], key)
        report = preflight(conn, plan)
        assert report.unprotected_retirements == (f"dataset_obs_{key}",)
        assert any("prepare_retirement" in f for f in report.expensive)

        swap(conn, plan)
        assert not has_bound_check(
            conn, f"dataset_obs_{key}", "observation_id", key
        )

    def test_prepare_retirement_protects_a_hand_made_partition(
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
        partitioned_db.execute(
            sa.text(
                f"CREATE TABLE dataset_obs_{key} PARTITION OF dataset "
                f"FOR VALUES IN ({key})"
            )
        )
        _dataset_row(
            partitioned_db, f"dataset_obs_{key}", key, targets[0], 1.0
        )
        conn = partitioned_db.connection()
        dataset = ensure_staging_table(conn, "dataset", key, 1).table
        build_partition_indexes(conn, "dataset", dataset)
        mirror_outbound_foreign_keys(conn, "dataset", dataset)
        plan = plan_swap(conn, [("dataset", dataset)], key)

        added = prepare_retirement(conn, plan)

        assert added == (f"dataset_obs_{key}_partcheck",)
        assert preflight(conn, plan).unprotected_retirements == ()
        assert prepare_retirement(conn, plan) == ()

    def test_prepare_retirement_is_a_no_op_for_provisioned_partitions(
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

        assert preflight(conn, plan).unprotected_retirements == ()
        assert prepare_retirement(conn, plan) == ()

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
        """An orphan makes the link attach fail, so nothing lands."""
        key = sample_observation.id
        targets = (
            sample_target.id,
            _extra_target(partitioned_db, sample_target, 1),
        )
        _provision_live(partitioned_db, key, targets, 1.0)
        dataset, link = _stage(partitioned_db, key, 1, targets, 9.9)
        partitioned_db.execute(
            sa.text(
                f"INSERT INTO {link} VALUES "
                "(:key, :source, 0, 0, :key, 987654321, 0, 0)"
            ),
            {"key": key, "source": targets[0]},
        )
        partitioned_db.commit()
        conn = partitioned_db.connection()

        with pytest.raises(sa.exc.IntegrityError):
            swap(
                conn,
                plan_swap(conn, list(zip(PAIR, (dataset, link))), key),
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

        detach_partition(conn, "datasetlink", f"datasetlink_obs_{key}")
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
