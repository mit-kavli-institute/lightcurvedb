"""Test the Alias symmetric cross-identification model.

Aliasing pairs two targets that are believed to refer to the same
astronomical object. The relation is symmetric (no direction) and explicitly
*not* transitive -- in a catalog split, one target may alias several others
without those others aliasing each other.
"""

import uuid

import pytest
from hypothesis import assume, given
from sqlalchemy import delete, exc, orm

from lightcurvedb.models import Alias, Mission, MissionCatalog, Target

from .strategies import tess as tess_st


def _make_catalog(
    session: orm.Session, name: str, catalog_name: str = "CAT"
) -> MissionCatalog:
    """Persist a mission + catalog and return the catalog."""
    mission = Mission(
        id=uuid.uuid4(),
        name=name,
        description="Alias test mission",
        time_unit="day",
        time_epoch=0.0,
        time_epoch_scale="tdb",
        time_epoch_format="jd",
        time_format_name=name,  # unique per mission
    )
    catalog = MissionCatalog(
        host_mission=mission, name=catalog_name, description="Alias catalog"
    )
    session.add_all([mission, catalog])
    session.flush()
    return catalog


def _make_target(
    session: orm.Session, catalog: MissionCatalog, name: int
) -> Target:
    """Persist and return a target in the given catalog."""
    target = Target(catalog=catalog, name=name)
    session.add(target)
    session.flush()
    return target


class TestAliasBetween:
    """Pure-logic tests for the Alias.between constructor."""

    @given(tess_st.tic_ids())
    def test_rejects_self_alias(self, identifier: int):
        """A target cannot be aliased to itself."""
        target = Target(id=identifier, name=identifier)
        with pytest.raises(ValueError):
            Alias.between(target, target)

    @given(tess_st.tic_ids(), tess_st.tic_ids())
    def test_rejects_distinct_objects_sharing_id(self, id_a: int, id_b: int):
        """Two targets with the same id are the same object, so disallowed."""
        a = Target(id=id_a, name=id_a)
        b = Target(id=id_a, name=id_b)  # deliberately reuse id_a
        with pytest.raises(ValueError):
            Alias.between(a, b)

    @given(tess_st.tic_ids(), tess_st.tic_ids())
    def test_links_both_targets(self, id_a: int, id_b: int):
        """between() wires up both relationship sides for a valid pair."""
        assume(id_a != id_b)
        a = Target(id=id_a, name=id_a)
        b = Target(id=id_b, name=id_b)

        alias = Alias.between(a, b)

        # Membership is order-agnostic; the pairing holds both targets.
        assert {alias.target, alias.counterpart} == {a, b}


class TestAliasBasics:
    """Create aliases and navigate them symmetrically."""

    def test_create_alias(self, v2_db: orm.Session):
        """A persisted alias links two existing targets."""
        catalog = _make_catalog(v2_db, "CREATE_MISSION")
        a = _make_target(v2_db, catalog, 1001)
        b = _make_target(v2_db, catalog, 1002)

        alias = Alias.between(a, b)
        v2_db.add(alias)
        v2_db.commit()

        assert alias.id is not None
        assert {alias.target_id, alias.counterpart_id} == {a.id, b.id}

    def test_symmetric_navigation(self, v2_db: orm.Session):
        """aliased_targets returns the other member regardless of column."""
        catalog = _make_catalog(v2_db, "NAV_MISSION")
        a = _make_target(v2_db, catalog, 2001)
        b = _make_target(v2_db, catalog, 2002)
        c = _make_target(v2_db, catalog, 2003)

        # a sits on the `target` side of both pairings.
        v2_db.add_all([Alias.between(a, b), Alias.between(a, c)])
        v2_db.commit()
        for t in (a, b, c):
            v2_db.refresh(t)

        assert {t.id for t in a.aliased_targets} == {b.id, c.id}
        assert {t.id for t in b.aliased_targets} == {a.id}
        assert {t.id for t in c.aliased_targets} == {a.id}
        assert len(a.aliases) == 2

    def test_navigation_independent_of_argument_order(
        self, v2_db: orm.Session
    ):
        """Either member reaches the other, whichever column it landed in."""
        catalog = _make_catalog(v2_db, "ORDER_MISSION")
        a = _make_target(v2_db, catalog, 3001)
        b = _make_target(v2_db, catalog, 3002)

        # Build with `a` deliberately on the counterpart side.
        v2_db.add(Alias.between(b, a))
        v2_db.commit()
        v2_db.refresh(a)
        v2_db.refresh(b)

        assert [t.id for t in a.aliased_targets] == [b.id]
        assert [t.id for t in b.aliased_targets] == [a.id]


class TestAliasConstraints:
    """Database-level guarantees: no self-aliases, no duplicate pairs, FKs."""

    def test_self_reference_rejected_by_db(self, v2_db: orm.Session):
        """The CHECK constraint rejects a row pointing a target at itself."""
        catalog = _make_catalog(v2_db, "SELF_MISSION")
        a = _make_target(v2_db, catalog, 4001)

        v2_db.add(Alias(target_id=a.id, counterpart_id=a.id))
        with pytest.raises(exc.IntegrityError):
            v2_db.commit()
        v2_db.rollback()

    def test_reversed_pair_is_duplicate(self, v2_db: orm.Session):
        """(b, a) is the same unordered pair as (a, b) and is rejected."""
        catalog = _make_catalog(v2_db, "REVERSE_MISSION")
        a = _make_target(v2_db, catalog, 5001)
        b = _make_target(v2_db, catalog, 5002)

        v2_db.add(Alias(target_id=a.id, counterpart_id=b.id))
        v2_db.commit()

        v2_db.add(Alias(target_id=b.id, counterpart_id=a.id))
        with pytest.raises(exc.IntegrityError):
            v2_db.commit()
        v2_db.rollback()

    def test_identical_pair_is_duplicate(self, v2_db: orm.Session):
        """The same ordered pair cannot be stored twice."""
        catalog = _make_catalog(v2_db, "DUP_MISSION")
        a = _make_target(v2_db, catalog, 6001)
        b = _make_target(v2_db, catalog, 6002)

        v2_db.add(Alias(target_id=a.id, counterpart_id=b.id))
        v2_db.commit()

        v2_db.add(Alias(target_id=a.id, counterpart_id=b.id))
        with pytest.raises(exc.IntegrityError):
            v2_db.commit()
        v2_db.rollback()

    def test_distinct_pairs_allowed(self, v2_db: orm.Session):
        """Different unordered pairs sharing a target coexist."""
        catalog = _make_catalog(v2_db, "DISTINCT_MISSION")
        a = _make_target(v2_db, catalog, 7001)
        b = _make_target(v2_db, catalog, 7002)
        c = _make_target(v2_db, catalog, 7003)

        v2_db.add_all([Alias.between(a, b), Alias.between(a, c)])
        v2_db.commit()  # should not raise

        assert v2_db.query(Alias).count() == 2

    def test_foreign_key_constraint(self, v2_db: orm.Session):
        """An alias referencing a non-existent target is rejected."""
        v2_db.add(Alias(target_id=999999, counterpart_id=888888))
        with pytest.raises(exc.IntegrityError):
            v2_db.commit()
        v2_db.rollback()

    def test_cascade_on_target_delete(self, v2_db: orm.Session):
        """Deleting a target removes its alias rows via ON DELETE CASCADE."""
        catalog = _make_catalog(v2_db, "CASCADE_MISSION")
        a = _make_target(v2_db, catalog, 8001)
        b = _make_target(v2_db, catalog, 8002)

        v2_db.add(Alias(target_id=a.id, counterpart_id=b.id))
        v2_db.commit()
        assert v2_db.query(Alias).count() == 1

        # DELETE at the SQL level exercises the DB FK cascade, not ORM cascade.
        v2_db.execute(delete(Target).where(Target.id == a.id))
        v2_db.commit()

        assert v2_db.query(Alias).count() == 0


class TestAliasSemantics:
    """Encode the domain cases that motivated the symmetric, loose model."""

    def test_cross_catalog_alias(self, v2_db: orm.Session):
        """The common case: the same object under two catalog ids."""
        mission = Mission(
            id=uuid.uuid4(),
            name="XCAT_MISSION",
            description="Cross-catalog mission",
            time_unit="day",
            time_epoch=0.0,
            time_epoch_scale="tdb",
            time_epoch_format="jd",
            time_format_name="XCAT_MISSION",
        )
        old_catalog = MissionCatalog(
            host_mission=mission, name="OLD", description="Older catalog"
        )
        new_catalog = MissionCatalog(
            host_mission=mission, name="NEW", description="Modern catalog"
        )
        v2_db.add_all([mission, old_catalog, new_catalog])
        v2_db.flush()

        old_entry = _make_target(v2_db, old_catalog, 12345)
        new_entry = _make_target(v2_db, new_catalog, 12345)

        v2_db.add(Alias.between(old_entry, new_entry))
        v2_db.commit()
        v2_db.refresh(old_entry)

        (counterpart,) = old_entry.aliased_targets
        assert counterpart.id == new_entry.id
        assert counterpart.catalog_id != old_entry.catalog_id

    def test_split_is_not_transitive(self, v2_db: orm.Session):
        """A target split into several does not alias them to each other.

        Old entry X resolves into modern entries Y and Z. X aliases both, but
        Y and Z are genuinely distinct objects and must not appear as aliases
        of one another.
        """
        catalog = _make_catalog(v2_db, "SPLIT_MISSION")
        x = _make_target(v2_db, catalog, 9001)
        y = _make_target(v2_db, catalog, 9002)
        z = _make_target(v2_db, catalog, 9003)

        v2_db.add_all([Alias.between(x, y), Alias.between(x, z)])
        v2_db.commit()
        for t in (x, y, z):
            v2_db.refresh(t)

        assert {t.id for t in x.aliased_targets} == {y.id, z.id}
        # Non-transitive: Y and Z never see each other.
        assert z.id not in {t.id for t in y.aliased_targets}
        assert y.id not in {t.id for t in z.aliased_targets}
