"""Tests for the partition lifecycle and the states derived from it.

The transition map is pure and gets Hypothesis; everything that reads
``pg_catalog`` gets examples. Hypothesis is never combined with a
database fixture here -- the fixture is function-scoped, so every
example would share one database, one set of deterministic relation
names and one uncommitted transaction.
"""

import pytest
import sqlalchemy as sa
from hypothesis import given
from hypothesis import strategies as st
from sqlalchemy import orm

from lightcurvedb.core.partitions import (
    LEGAL_TRANSITIONS,
    IllegalStateTransitionError,
    OrbitState,
    PartitionName,
    PartitionNameError,
    RevisionSkewError,
    assert_paired_revisions,
    build_partition_indexes,
    create_staging_table,
    derive_state,
    detach_partition,
    ensure_partition,
    ensure_staging_table,
    live_revision,
    mirror_outbound_foreign_keys,
    next_revision,
    revisions_of,
    validate_transition,
)
from lightcurvedb.models import Observation, Target

states = st.sampled_from(list(OrbitState))


class TestTransitionMap:
    """Pure; no database."""

    def test_every_state_has_an_entry(self):
        assert set(LEGAL_TRANSITIONS) == set(OrbitState)

    def test_gone_is_the_only_terminal_state(self):
        terminal = {
            state
            for state, targets in LEGAL_TRANSITIONS.items()
            if not targets
        }
        assert terminal == {OrbitState.GONE}

    def test_every_state_is_reachable_from_absent(self):
        reached = {OrbitState.ABSENT}
        frontier = [OrbitState.ABSENT]
        while frontier:
            for target in LEGAL_TRANSITIONS[frontier.pop()]:
                if target not in reached:
                    reached.add(target)
                    frontier.append(target)
        assert reached == set(OrbitState)

    @given(states)
    def test_staying_put_is_always_legal(self, state: OrbitState):
        validate_transition(state, state)

    @given(states)
    def test_no_state_lists_itself(self, state: OrbitState):
        assert state not in LEGAL_TRANSITIONS[state]

    @given(states, states)
    def test_illegal_edges_raise(
        self, current: OrbitState, target: OrbitState
    ):
        if current is target or target in LEGAL_TRANSITIONS[current]:
            validate_transition(current, target)
            return
        with pytest.raises(IllegalStateTransitionError):
            validate_transition(current, target)

    def test_the_message_says_what_was_allowed(self):
        with pytest.raises(IllegalStateTransitionError, match="created"):
            validate_transition(OrbitState.ABSENT, OrbitState.LIVE)


@pytest.mark.partitioning
class TestDeriveState:
    @pytest.fixture
    def orm_session(self, partitioned_db):
        return partitioned_db

    def test_walks_the_ladder_as_the_table_is_prepared(
        self,
        partitioned_db: orm.Session,
        sample_observation: Observation,
        sample_target: Target,
    ):
        conn = partitioned_db.connection()
        key = sample_observation.id
        name = PartitionName("dataset", key, 1).table

        assert derive_state(conn, "dataset", key, 1) is OrbitState.ABSENT

        create_staging_table(conn, "dataset", name, key)
        assert derive_state(conn, "dataset", key, 1) is OrbitState.CREATED

        partitioned_db.execute(
            sa.text(
                f"INSERT INTO {name} (observation_id, target_id, "
                "photometric_method_id, processing_method_id, values) "
                "VALUES (:obs, :target, 0, 0, ARRAY[1.0]::float8[])"
            ),
            {"obs": key, "target": sample_target.id},
        )
        assert derive_state(conn, "dataset", key, 1) is OrbitState.LOADED

        build_partition_indexes(conn, "dataset", name)
        assert derive_state(conn, "dataset", key, 1) is OrbitState.INDEXED

        mirror_outbound_foreign_keys(conn, "dataset", name)
        assert derive_state(conn, "dataset", key, 1) is OrbitState.FK_READY

    def test_an_attached_partition_is_live(
        self, partitioned_db: orm.Session, sample_observation: Observation
    ):
        conn = partitioned_db.connection()
        key = sample_observation.id
        ensure_partition(conn, "dataset", key, revision=1)

        assert derive_state(conn, "dataset", key, 1) is OrbitState.LIVE

    def test_hierarchy_reaches_fk_ready_with_no_keys_to_mirror(
        self, partitioned_db: orm.Session
    ):
        """Its only keys point at a partitioned table, so none are due."""
        conn = partitioned_db.connection()
        name = ensure_staging_table(conn, "datasethierarchy", 7, 1).table
        partitioned_db.execute(
            sa.text(f"INSERT INTO {name} VALUES (7, 1, 0, 0, 7, 2, 0, 0)")
        )
        build_partition_indexes(conn, "datasethierarchy", name)

        assert (
            derive_state(conn, "datasethierarchy", 7, 1) is OrbitState.FK_READY
        )


@pytest.mark.partitioning
class TestRevisions:
    @pytest.fixture
    def orm_session(self, partitioned_db):
        return partitioned_db

    def test_unprovisioned_slots_start_at_revision_zero(
        self, partitioned_db: orm.Session
    ):
        conn = partitioned_db.connection()
        assert live_revision(conn, "dataset", 7) is None
        assert next_revision(conn, "dataset", 7) == 0

    def test_a_hand_made_partition_reads_as_revision_zero(
        self, partitioned_db: orm.Session, sample_observation: Observation
    ):
        """The CHANGELOG's DBA recipe, adopted without special casing."""
        conn = partitioned_db.connection()
        key = sample_observation.id
        partitioned_db.execute(
            sa.text(
                f"CREATE TABLE dataset_obs_{key} PARTITION OF dataset "
                f"FOR VALUES IN ({key})"
            )
        )

        assert live_revision(conn, "dataset", key) == 0
        assert next_revision(conn, "dataset", key) == 1

    def test_revisions_accumulate_across_campaigns(
        self, partitioned_db: orm.Session, sample_observation: Observation
    ):
        """A later campaign never reuses a number an earlier one took."""
        conn = partitioned_db.connection()
        key = sample_observation.id
        ensure_partition(conn, "dataset", key)
        ensure_staging_table(conn, "dataset", key, 1)

        assert next_revision(conn, "dataset", key) == 2

        ensure_staging_table(conn, "dataset", key, 2)
        assert next_revision(conn, "dataset", key) == 3
        assert set(revisions_of(conn, "dataset", key)) == {0, 1, 2}

    def test_a_retired_relation_still_holds_its_number(
        self, partitioned_db: orm.Session, sample_observation: Observation
    ):
        conn = partitioned_db.connection()
        key = sample_observation.id
        name = ensure_partition(conn, "dataset", key, revision=1).table
        detach_partition(conn, "dataset", name)

        assert live_revision(conn, "dataset", key) is None
        assert next_revision(conn, "dataset", key) == 2

    def test_a_neighbouring_observation_id_is_not_counted(
        self, partitioned_db: orm.Session
    ):
        """``dataset_obs_70_v9`` is not a revision of observation 7."""
        conn = partitioned_db.connection()
        ensure_staging_table(conn, "dataset", 70, 9)

        assert revisions_of(conn, "dataset", 7) == {}
        assert next_revision(conn, "dataset", 7) == 0

    def test_a_partition_named_off_scheme_has_no_revision(
        self, partitioned_db: orm.Session, sample_observation: Observation
    ):
        conn = partitioned_db.connection()
        key = sample_observation.id
        partitioned_db.execute(
            sa.text(
                f"CREATE TABLE ds_handmade PARTITION OF dataset "
                f"FOR VALUES IN ({key})"
            )
        )

        with pytest.raises(PartitionNameError, match="naming scheme"):
            live_revision(conn, "dataset", key)


@pytest.mark.partitioning
class TestPairedRevisions:
    @pytest.fixture
    def orm_session(self, partitioned_db):
        return partitioned_db

    def test_agreement_returns_the_shared_revision(
        self, partitioned_db: orm.Session, sample_observation: Observation
    ):
        conn = partitioned_db.connection()
        key = sample_observation.id
        ensure_partition(conn, "dataset", key, revision=3)
        ensure_partition(conn, "datasethierarchy", key, revision=3)

        found = assert_paired_revisions(
            conn, ["dataset", "datasethierarchy"], key
        )
        assert found == 3

    def test_unprovisioned_on_both_sides_is_not_skew(
        self, partitioned_db: orm.Session
    ):
        conn = partitioned_db.connection()
        assert (
            assert_paired_revisions(conn, ["dataset", "datasethierarchy"], 7)
            is None
        )

    def test_skew_raises_and_names_both_sides(
        self, partitioned_db: orm.Session, sample_observation: Observation
    ):
        """A swap that was not atomic is the only way to get here."""
        conn = partitioned_db.connection()
        key = sample_observation.id
        ensure_partition(conn, "dataset", key, revision=4)
        ensure_partition(conn, "datasethierarchy", key, revision=3)

        with pytest.raises(RevisionSkewError, match="dataset at 4"):
            assert_paired_revisions(conn, ["dataset", "datasethierarchy"], key)

    def test_one_side_missing_is_skew(
        self, partitioned_db: orm.Session, sample_observation: Observation
    ):
        conn = partitioned_db.connection()
        key = sample_observation.id
        ensure_partition(conn, "dataset", key, revision=1)

        with pytest.raises(RevisionSkewError, match="nothing"):
            assert_paired_revisions(conn, ["dataset", "datasethierarchy"], key)
