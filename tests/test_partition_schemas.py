"""Tests for partitions living in a schema of their own.

``schema`` names the partitioned parent; ``partition_schema`` names where
its children go. They are the same by default, which is what every other
partition test exercises. Here they differ -- the layout a deployment
adopts to keep ``\\dt`` readable -- and the interesting cases are the ones
where the two are easy to confuse: the ``pg_class`` sweeps that see one
schema at a time, and the swap, whose retiring side is discovered rather
than told.
"""

import pytest
import sqlalchemy as sa
from sqlalchemy import orm

from lightcurvedb.core.partitions import (
    PartitionError,
    RevisionState,
    StagingShapeMismatchError,
    SwapPair,
    SwapResult,
    SwapStep,
    analyze_relation,
    build_partition_indexes,
    check_attachable,
    derive_state,
    drop_retired,
    ensure_default_partition,
    ensure_partition,
    ensure_staging_table,
    find_partition_for_value,
    live_revision,
    mirror_outbound_foreign_keys,
    next_revision,
    plan_swap,
    preflight,
    prepare_retirement,
    revisions_of,
    rollback_swap,
    swap,
    verify_relation_shape,
)
from lightcurvedb.models import Observation, Target

pytestmark = pytest.mark.partitioning

PAIR = ("dataset", "datasethierarchy")
SCHEMA = "_partitions"


@pytest.fixture
def orm_session(partitioned_db):
    return partitioned_db


@pytest.fixture
def partition_schema(partitioned_db, database_engine):
    """A schema for child partitions, torn down by hand.

    ``drop_unmanaged_relations`` sweeps ``public`` only, so a detached
    relation left here would outlive the test and its foreign key to
    ``observation`` would then make ``metadata.drop_all`` fail. Depending
    on ``partitioned_db`` puts this teardown before the engine's.
    """
    partitioned_db.execute(sa.text(f'CREATE SCHEMA "{SCHEMA}"'))
    partitioned_db.commit()
    try:
        yield SCHEMA
    finally:
        partitioned_db.rollback()
        with database_engine.connect() as conn:
            conn.execute(sa.text(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE'))
            conn.commit()


def _relation_schema(session: orm.Session, relation: str) -> str | None:
    return session.execute(
        sa.text(
            "SELECT n.nspname FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE c.relname = :name"
        ),
        {"name": relation},
    ).scalar()


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
    other = Target(catalog_id=target.catalog_id, name=target.name + offset)
    session.add(other)
    session.flush()
    return other.id


def _provision_live(
    session: orm.Session,
    key: int,
    targets: tuple[int, int],
    value: float,
    *,
    partition_schema: str | None = None,
) -> tuple[str, str]:
    """A live revision 0 for both parents, holding ``value``."""
    conn = session.connection()
    where = partition_schema or "public"
    names = tuple(
        ensure_partition(
            conn, parent, key, partition_schema=partition_schema
        ).table
        for parent in PAIR
    )
    for target in targets:
        _dataset_row(session, f"{where}.{names[0]}", key, target, value)
    _hierarchy_row(session, f"{where}.{names[1]}", key, *targets)
    return names


def _stage(
    session: orm.Session,
    key: int,
    revision: int,
    targets: tuple[int, int],
    value: float,
    *,
    partition_schema: str = SCHEMA,
) -> tuple[str, str]:
    """A fully prepared revision in ``partition_schema``."""
    conn = session.connection()
    names = tuple(
        ensure_staging_table(
            conn, parent, key, revision, partition_schema=partition_schema
        ).table
        for parent in PAIR
    )
    for target in targets:
        _dataset_row(
            session, f"{partition_schema}.{names[0]}", key, target, value
        )
    _hierarchy_row(session, f"{partition_schema}.{names[1]}", key, *targets)

    for parent, name in zip(PAIR, names):
        build_partition_indexes(
            conn, parent, name, partition_schema=partition_schema
        )
        analyze_relation(conn, name, schema=partition_schema)
    mirror_outbound_foreign_keys(
        conn, "dataset", names[0], partition_schema=partition_schema
    )
    return names


def _first_value(session: orm.Session, relation: str, key: int) -> float:
    return session.execute(
        sa.text(
            f"SELECT DISTINCT values[1] FROM {relation} "
            "WHERE observation_id = :key"
        ),
        {"key": key},
    ).scalar()


class TestProvisioning:
    def test_the_child_lands_in_the_partition_schema(
        self,
        partitioned_db: orm.Session,
        partition_schema: str,
        sample_observation: Observation,
    ):
        conn = partitioned_db.connection()
        name = ensure_partition(
            conn, "dataset", sample_observation.id, partition_schema=SCHEMA
        )
        assert _relation_schema(partitioned_db, name.table) == SCHEMA

    def test_the_read_path_reports_where_it_really_is(
        self,
        partitioned_db: orm.Session,
        partition_schema: str,
        sample_observation: Observation,
    ):
        """No argument needed: ``pg_inherits`` does not care about schemas."""
        conn = partitioned_db.connection()
        key = sample_observation.id
        ensure_partition(conn, "dataset", key, partition_schema=SCHEMA)

        live = find_partition_for_value(conn, "dataset", key)
        assert live is not None
        assert live.schema == SCHEMA
        assert live.qualified_name == f"{SCHEMA}.dataset_obs_{key}"

    def test_omitting_it_still_uses_the_parents_schema(
        self, partitioned_db: orm.Session, sample_observation: Observation
    ):
        conn = partitioned_db.connection()
        name = ensure_partition(conn, "dataset", sample_observation.id)
        assert _relation_schema(partitioned_db, name.table) == "public"

    def test_it_refuses_the_same_name_in_another_schema(
        self,
        partitioned_db: orm.Session,
        partition_schema: str,
        sample_observation: Observation,
    ):
        """A relation name in two schemas is two different relations."""
        conn = partitioned_db.connection()
        key = sample_observation.id
        ensure_partition(conn, "dataset", key, partition_schema=SCHEMA)

        with pytest.raises(PartitionError, match="is a swap"):
            ensure_partition(conn, "dataset", key)

    def test_the_default_partition_is_probed_where_it_lives(
        self,
        partitioned_db: orm.Session,
        partition_schema: str,
        sample_observation: Observation,
    ):
        """Its name comes from an oid, so its schema must come from one too."""
        conn = partitioned_db.connection()
        key = sample_observation.id
        ensure_default_partition(conn, "dataset", partition_schema=SCHEMA)
        assert _relation_schema(partitioned_db, "dataset_default") == SCHEMA

        report = check_attachable(
            conn,
            "dataset",
            "dataset_obs_1_v1",
            key,
            partition_schema=SCHEMA,
        )
        assert report.default_partition == "dataset_default"
        assert not report.default_partition_conflicts


class TestStaging:
    def test_staging_tables_land_in_the_partition_schema(
        self,
        partitioned_db: orm.Session,
        partition_schema: str,
        sample_observation: Observation,
    ):
        conn = partitioned_db.connection()
        name = ensure_staging_table(
            conn, "dataset", sample_observation.id, 1, partition_schema=SCHEMA
        )
        assert name.table == f"dataset_obs_{sample_observation.id}_v1"
        assert _relation_schema(partitioned_db, name.table) == SCHEMA

    def test_indexes_and_keys_are_built_beside_their_table(
        self,
        partitioned_db: orm.Session,
        partition_schema: str,
        sample_observation: Observation,
        sample_target: Target,
    ):
        key = sample_observation.id
        targets = (
            sample_target.id,
            _extra_target(partitioned_db, sample_target, 1),
        )
        dataset, _ = _stage(partitioned_db, key, 1, targets, 2.0)
        conn = partitioned_db.connection()

        assert _relation_schema(partitioned_db, f"{dataset}_pkey") == SCHEMA
        report = check_attachable(
            conn, "dataset", dataset, key, partition_schema=SCHEMA
        )
        assert report.blocking == ()
        assert report.candidate_schema == SCHEMA
        assert report.parent_schema == "public"
        assert report.qualified_candidate == f"{SCHEMA}.{dataset}"

    def test_without_the_argument_the_candidate_reads_as_absent(
        self,
        partitioned_db: orm.Session,
        partition_schema: str,
        sample_observation: Observation,
        sample_target: Target,
    ):
        """A blocking finding, not a crash."""
        key = sample_observation.id
        targets = (
            sample_target.id,
            _extra_target(partitioned_db, sample_target, 1),
        )
        dataset, _ = _stage(partitioned_db, key, 1, targets, 2.0)

        report = check_attachable(
            partitioned_db.connection(), "dataset", dataset, key
        )
        assert report.candidate_kind is None
        assert any("absent" in finding for finding in report.blocking)

    def test_a_shape_mismatch_names_both_schemas(
        self,
        partitioned_db: orm.Session,
        partition_schema: str,
        sample_observation: Observation,
    ):
        partitioned_db.execute(
            sa.text(f"CREATE TABLE {SCHEMA}.wrong_shape (id integer)")
        )
        with pytest.raises(StagingShapeMismatchError) as caught:
            verify_relation_shape(
                partitioned_db.connection(),
                "dataset",
                "wrong_shape",
                partition_schema=SCHEMA,
            )
        assert f"{SCHEMA}.wrong_shape" in str(caught.value)
        assert "public.dataset" in str(caught.value)


class TestRevisions:
    def test_each_schema_is_swept_on_its_own(
        self,
        partitioned_db: orm.Session,
        partition_schema: str,
        sample_observation: Observation,
    ):
        """The sweep that was the proof ``schema`` meant two things."""
        conn = partitioned_db.connection()
        key = sample_observation.id
        ensure_partition(conn, "dataset", key, partition_schema=SCHEMA)
        ensure_staging_table(conn, "dataset", key, 1, partition_schema=SCHEMA)

        assert revisions_of(conn, "dataset", key) == {}
        assert next_revision(conn, "dataset", key) == 0
        assert set(
            revisions_of(conn, "dataset", key, partition_schema=SCHEMA)
        ) == {0, 1}
        assert (
            next_revision(conn, "dataset", key, partition_schema=SCHEMA) == 2
        )

    def test_live_revision_needs_no_argument(
        self,
        partitioned_db: orm.Session,
        partition_schema: str,
        sample_observation: Observation,
    ):
        """A revision number carries no schema."""
        conn = partitioned_db.connection()
        key = sample_observation.id
        ensure_partition(conn, "dataset", key, partition_schema=SCHEMA)
        assert live_revision(conn, "dataset", key) == 0

    def test_state_is_derived_across_the_split(
        self,
        partitioned_db: orm.Session,
        partition_schema: str,
        sample_observation: Observation,
        sample_target: Target,
    ):
        conn = partitioned_db.connection()
        key = sample_observation.id
        targets = (
            sample_target.id,
            _extra_target(partitioned_db, sample_target, 1),
        )

        def state(revision: int) -> RevisionState:
            return derive_state(
                conn, "dataset", key, revision, partition_schema=SCHEMA
            )

        assert state(1) is RevisionState.ABSENT
        ensure_staging_table(conn, "dataset", key, 1, partition_schema=SCHEMA)
        assert state(1) is RevisionState.CREATED

        _dataset_row(
            partitioned_db,
            f"{SCHEMA}.dataset_obs_{key}_v1",
            key,
            targets[0],
            2.0,
        )
        assert state(1) is RevisionState.LOADED

        build_partition_indexes(
            conn, "dataset", f"dataset_obs_{key}_v1", partition_schema=SCHEMA
        )
        assert state(1) is RevisionState.INDEXED

        mirror_outbound_foreign_keys(
            conn, "dataset", f"dataset_obs_{key}_v1", partition_schema=SCHEMA
        )
        assert state(1) is RevisionState.FK_READY

    def test_a_revision_in_the_parents_schema_is_not_seen(
        self,
        partitioned_db: orm.Session,
        partition_schema: str,
        sample_observation: Observation,
    ):
        """One schema is swept, and it is the one you asked about."""
        conn = partitioned_db.connection()
        key = sample_observation.id
        ensure_partition(conn, "dataset", key)

        assert set(revisions_of(conn, "dataset", key)) == {0}
        assert (
            revisions_of(conn, "dataset", key, partition_schema=SCHEMA) == {}
        )


class TestSwap:
    def test_a_swap_entirely_inside_the_partition_schema(
        self,
        partitioned_db: orm.Session,
        partition_schema: str,
        sample_observation: Observation,
        sample_target: Target,
    ):
        key = sample_observation.id
        targets = (
            sample_target.id,
            _extra_target(partitioned_db, sample_target, 1),
        )
        _provision_live(
            partitioned_db, key, targets, 1.0, partition_schema=SCHEMA
        )
        staged = _stage(partitioned_db, key, 1, targets, 9.9)
        conn = partitioned_db.connection()

        plan = plan_swap(
            conn,
            list(zip(PAIR, staged)),
            key,
            partition_schema=SCHEMA,
        )
        assert plan.schema == "public"
        assert plan.partition_schema == SCHEMA
        assert [pair.retiring_schema for pair in plan.pairs] == [SCHEMA] * 2

        report = preflight(conn, plan)
        assert report.blocking == ()

        prepare_retirement(conn, plan)
        result = swap(conn, plan)
        partitioned_db.commit()

        assert result.partition_schema == SCHEMA
        assert _first_value(partitioned_db, "dataset", key) == 9.9
        assert (
            find_partition_for_value(
                partitioned_db.connection(), "dataset", key
            ).name
            == staged[0]
        )
        # The retired relation is still on disk and still readable.
        assert (
            _first_value(partitioned_db, f"{SCHEMA}.dataset_obs_{key}", key)
            == 1.0
        )

    def test_rollback_restores_the_earlier_revision(
        self,
        partitioned_db: orm.Session,
        partition_schema: str,
        sample_observation: Observation,
        sample_target: Target,
    ):
        key = sample_observation.id
        targets = (
            sample_target.id,
            _extra_target(partitioned_db, sample_target, 1),
        )
        _provision_live(
            partitioned_db, key, targets, 1.0, partition_schema=SCHEMA
        )
        staged = _stage(partitioned_db, key, 1, targets, 9.9)
        conn = partitioned_db.connection()

        plan = plan_swap(
            conn, list(zip(PAIR, staged)), key, partition_schema=SCHEMA
        )
        prepare_retirement(conn, plan)
        result = swap(conn, plan)
        partitioned_db.commit()

        rollback_swap(partitioned_db.connection(), result)
        partitioned_db.commit()
        assert _first_value(partitioned_db, "dataset", key) == 1.0

    def test_drop_retired_takes_the_partition_schema(
        self,
        partitioned_db: orm.Session,
        partition_schema: str,
        sample_observation: Observation,
        sample_target: Target,
    ):
        key = sample_observation.id
        targets = (
            sample_target.id,
            _extra_target(partitioned_db, sample_target, 1),
        )
        _provision_live(
            partitioned_db, key, targets, 1.0, partition_schema=SCHEMA
        )
        staged = _stage(partitioned_db, key, 1, targets, 9.9)
        conn = partitioned_db.connection()

        plan = plan_swap(
            conn, list(zip(PAIR, staged)), key, partition_schema=SCHEMA
        )
        prepare_retirement(conn, plan)
        result = swap(conn, plan)
        partitioned_db.commit()

        conn = partitioned_db.connection()
        listed = drop_retired(
            conn, result.retired, schema=result.partition_schema
        )
        assert set(listed) == set(result.retired)
        assert _relation_schema(partitioned_db, result.retired[0]) == SCHEMA

        drop_retired(
            conn,
            result.retired,
            schema=result.partition_schema,
            dry_run=False,
        )
        partitioned_db.commit()
        assert _relation_schema(partitioned_db, result.retired[0]) is None


class TestMigratingIntoASchema:
    """Live in ``public``, staged in ``_partitions``.

    The case that makes the retiring side discovered rather than told:
    a deployment moving its partitions has both schemas in play at once,
    and a single ``partition_schema`` cannot describe it.
    """

    def test_the_swap_moves_the_slot_across_schemas(
        self,
        partitioned_db: orm.Session,
        partition_schema: str,
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

        plan = plan_swap(
            conn, list(zip(PAIR, staged)), key, partition_schema=SCHEMA
        )
        assert plan.partition_schema == SCHEMA
        assert [pair.retiring_schema for pair in plan.pairs] == ["public"] * 2

        prepare_retirement(conn, plan)
        result = swap(conn, plan)
        partitioned_db.commit()

        # The result records both sides, so a rollback can cross back.
        assert result.partition_schema == SCHEMA
        assert [step.retired_schema for step in result.steps] == ["public"] * 2

        assert _first_value(partitioned_db, "dataset", key) == 9.9
        live = find_partition_for_value(
            partitioned_db.connection(), "dataset", key
        )
        assert live.schema == SCHEMA
        # The old partition is still in public, detached and readable.
        assert _relation_schema(partitioned_db, f"dataset_obs_{key}") == (
            "public"
        )

    def test_rollback_puts_it_back_in_the_parents_schema(
        self,
        partitioned_db: orm.Session,
        partition_schema: str,
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

        plan = plan_swap(
            conn, list(zip(PAIR, staged)), key, partition_schema=SCHEMA
        )
        prepare_retirement(conn, plan)
        result = swap(conn, plan)
        partitioned_db.commit()

        rollback_swap(partitioned_db.connection(), result)
        partitioned_db.commit()

        assert _first_value(partitioned_db, "dataset", key) == 1.0
        live = find_partition_for_value(
            partitioned_db.connection(), "dataset", key
        )
        assert live.schema == "public"

    def test_rollback_refuses_retired_relations_in_two_schemas(
        self, partitioned_db: orm.Session
    ):
        """One plan carries one ``partition_schema``, so it cannot."""
        result = SwapResult(
            key_value=1,
            schema="public",
            partition_schema=SCHEMA,
            steps=(
                SwapStep(
                    parent="dataset",
                    promoted="dataset_obs_1_v1",
                    retired="dataset_obs_1",
                    retired_schema="public",
                ),
                SwapStep(
                    parent="datasethierarchy",
                    promoted="datasethierarchy_obs_1_v1",
                    retired="datasethierarchy_obs_1",
                    retired_schema=SCHEMA,
                ),
            ),
        )
        with pytest.raises(PartitionError, match="spread across"):
            rollback_swap(partitioned_db.connection(), result)


def test_swap_pair_records_both_sides():
    """The dataclass carries a schema for each side, independently."""
    pair = SwapPair(
        parent="dataset",
        incoming="dataset_obs_1_v1",
        retiring="dataset_obs_1",
        retiring_schema="public",
    )
    assert pair.retiring_schema == "public"
