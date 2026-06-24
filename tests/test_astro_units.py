"""Tests that :class:`AstroUnit` serializes astropy units losslessly.

``AstroUnit`` stores a unit as ``str(unit)`` and rebuilds it with
``u.Unit(unit_str)``. These tests draw real astropy units (named and composite)
and assert the round-trip is exact -- in memory and through the database.

Equality uses ``UnitBase.__eq__``, which compares physical decomposition *and*
scale, so a serialization that lost (say) a ``km`` -> ``m`` scale factor would
fail rather than slip through (``is_equivalent`` would ignore that and is
therefore the wrong assertion here).
"""

import astropy.units as u
import pytest
from hypothesis import HealthCheck, given, settings
from sqlalchemy import delete, exc, orm, select

from lightcurvedb.models import (
    AstroParameter,
    AstroUnit,
    Mission,
    MissionCatalog,
    Target,
)

from .strategies import astro as astro_st


def _make_catalog(session: orm.Session, mission_name: str) -> MissionCatalog:
    """Persist a mission + catalog and return the catalog."""
    mission = Mission(
        name=mission_name,
        description="astro-parameter test mission",
        time_unit="day",
        time_epoch=0.0,
        time_epoch_scale="tdb",
        time_epoch_format="jd",
        time_format_name=mission_name,
    )
    catalog = MissionCatalog(
        host_mission=mission, name="CAT", description="astro-param catalog"
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


def _make_unit(session: orm.Session, astropy_unit, name: str) -> AstroUnit:
    """Persist and return an AstroUnit reflected from an astropy unit."""
    unit = AstroUnit.reflect_astropy_unit(astropy_unit, name=name)
    session.add(unit)
    session.flush()
    return unit


class TestAstroUnit:
    # --- pure reflection: no DB, no Hypothesis ------------------------------
    def test_reflect_from_unit(self):
        au = AstroUnit.reflect_astropy_unit(u.m, name="meter")
        assert au.unit_str == "m"
        assert au.as_unit() == u.m

    def test_reflect_from_composite_unit(self):
        au = AstroUnit.reflect_astropy_unit(u.m / u.s, name="velocity")
        assert au.as_unit() == u.m / u.s

    def test_reflect_from_quantity_uses_unit(self):
        # The scalar magnitude is intentionally dropped; only the unit is kept.
        au = AstroUnit.reflect_astropy_unit(3.0 * u.m, name="meter")
        assert au.unit_str == "m"
        assert au.as_unit() == u.m

    @pytest.mark.parametrize("bad", [1.0, "m", None, object()])
    def test_reflect_rejects_non_unit(self, bad):
        with pytest.raises(NotImplementedError):
            AstroUnit.reflect_astropy_unit(bad, name="x")

    # --- no-DB property: str <-> Unit round-trip ----------------------------
    @given(unit=astro_st.astropy_named_units())
    def test_named_unit_str_roundtrip(self, unit):
        assert u.Unit(str(unit)) == unit
        au = AstroUnit.reflect_astropy_unit(unit, name=str(unit))
        assert au.as_unit() == unit

    @given(unit=astro_st.astropy_composite_units())
    def test_composite_unit_str_roundtrip(self, unit):
        # The strategy filters to round-trippable units (excluding extreme
        # magnitudes where astropy's own == overflows/loses precision); this
        # asserts the contract holds for everything realistic it yields.
        assert u.Unit(str(unit)) == unit

    # --- DB round-trip: real Postgres session via v2_db ---------------------
    @given(unit=astro_st.astropy_named_units())
    @settings(
        # v2_db is function-scoped but @given runs many examples inside one
        # fixture setup; that sharing is deliberate here (see rollback below).
        suppress_health_check=[HealthCheck.function_scoped_fixture],
        max_examples=25,
        deadline=None,  # each example does a real commit
    )
    def test_db_roundtrip(self, v2_db: orm.Session, unit):
        au = AstroUnit.reflect_astropy_unit(unit, name=str(unit))
        v2_db.add(au)
        v2_db.commit()
        v2_db.refresh(au)
        assert au.id is not None

        fetched = v2_db.execute(
            select(AstroUnit).where(AstroUnit.id == au.id)
        ).scalar_one()
        assert fetched.unit_str == str(unit)
        assert fetched.as_unit() == unit

        # Clear the transaction so the next example starts fresh. Committed
        # rows are harmless: unit_str has no unique constraint, so duplicate
        # examples cannot collide.
        v2_db.rollback()


class TestAstroParameter:
    """Persistence, relationships, and name-keyed access for AstroParameter.

    Scale/unit *correctness* is a developer contract: the layer stores
    ``value`` together with its unit and returns both verbatim, performing no
    conversion or normalization. A parameter's ``name`` is read-only, mirrored
    from its unit's name, so the kind is set on the :class:`AstroUnit`; with
    ``unique(target_id, unit_id)`` a target holds one parameter per kind,
    reachable by keyword through ``target.parameters_by_name`` (or
    ``target[name]``).
    """

    def test_quantity_roundtrip_value_and_unit(self, v2_db: orm.Session):
        catalog = _make_catalog(v2_db, "PARAM_QTY")
        target = _make_target(v2_db, catalog, 1001)
        # The unit carries the parameter kind as its (unique) name.
        unit = _make_unit(v2_db, u.K, "effective_temperature")
        param = AstroParameter(
            target=target,
            unit=unit,
            value=5772.0,
            upper_error=50.0,
            lower_error=40.0,
        )
        v2_db.add(param)
        v2_db.commit()

        fetched = v2_db.execute(
            select(AstroParameter).where(AstroParameter.id == param.id)
        ).scalar_one()
        assert fetched.value == 5772.0
        assert fetched.name == "effective_temperature"  # mirrored unit.name
        assert fetched.unit.as_unit() == u.K
        assert fetched.as_quantity() == 5772.0 * u.K

    def test_asymmetric_errors_preserved(self, v2_db: orm.Session):
        catalog = _make_catalog(v2_db, "PARAM_ERR")
        target = _make_target(v2_db, catalog, 2001)
        unit = _make_unit(v2_db, u.day, "period")
        param = AstroParameter(
            target=target,
            unit=unit,
            value=3.5,
            upper_error=0.2,
            lower_error=0.1,
        )
        v2_db.add(param)
        v2_db.commit()

        fetched = v2_db.execute(
            select(AstroParameter).where(AstroParameter.id == param.id)
        ).scalar_one()
        # The two errors survive independently (not collapsed or swapped).
        assert fetched.upper_error == 0.2
        assert fetched.lower_error == 0.1

    def test_value_and_unit_stored_verbatim(self, v2_db: orm.Session):
        catalog = _make_catalog(v2_db, "PARAM_VERBATIM")
        target = _make_target(v2_db, catalog, 3001)
        # km deliberately NOT normalized to m: the layer stores it as-is.
        unit = _make_unit(v2_db, u.km, "distance")
        param = AstroParameter(
            target=target,
            unit=unit,
            value=149.6e6,
            upper_error=0.1e6,
            lower_error=0.1e6,
        )
        v2_db.add(param)
        v2_db.commit()

        fetched = v2_db.execute(
            select(AstroParameter).where(AstroParameter.id == param.id)
        ).scalar_one()
        assert fetched.value == 149.6e6  # not 1.496e11
        assert fetched.unit.unit_str == "km"
        assert fetched.as_quantity() == 149.6e6 * u.km

    def test_target_parameters_navigation(self, v2_db: orm.Session):
        catalog = _make_catalog(v2_db, "NAV_PARAMS")
        target = _make_target(v2_db, catalog, 4001)
        temp = _make_unit(v2_db, u.K, "effective_temperature")
        period = _make_unit(v2_db, u.day, "period")
        v2_db.add_all(
            [
                AstroParameter(
                    target=target,
                    unit=temp,
                    value=5772.0,
                    upper_error=1.0,
                    lower_error=1.0,
                ),
                AstroParameter(
                    target=target,
                    unit=period,
                    value=3.5,
                    upper_error=0.1,
                    lower_error=0.1,
                ),
            ]
        )
        v2_db.commit()
        v2_db.refresh(target)

        assert len(target.parameters) == 2
        units = [p.unit.as_unit() for p in target.parameters]
        assert u.K in units
        assert u.day in units

    def test_parameters_by_name_keyed_access(self, v2_db: orm.Session):
        catalog = _make_catalog(v2_db, "BY_NAME")
        target = _make_target(v2_db, catalog, 8001)
        temp = _make_unit(v2_db, u.K, "effective_temperature")
        radius = _make_unit(v2_db, u.solRad, "radius")
        v2_db.add_all(
            [
                AstroParameter(
                    target=target,
                    unit=temp,
                    value=5772.0,
                    upper_error=50.0,
                    lower_error=40.0,
                ),
                AstroParameter(
                    target=target,
                    unit=radius,
                    value=1.0,
                    upper_error=0.1,
                    lower_error=0.1,
                ),
            ]
        )
        v2_db.commit()
        v2_db.refresh(target)

        by_name = target.parameters_by_name
        assert set(by_name) == {"effective_temperature", "radius"}
        assert by_name["effective_temperature"].value == 5772.0
        assert by_name["effective_temperature"].as_quantity() == 5772.0 * u.K
        assert by_name["radius"].unit.as_unit() == u.solRad

    def test_getitem_returns_quantity_by_name(self, v2_db: orm.Session):
        catalog = _make_catalog(v2_db, "GETITEM")
        target = _make_target(v2_db, catalog, 11001)
        temp = _make_unit(v2_db, u.K, "effective_temperature")
        v2_db.add(
            AstroParameter(
                target=target,
                unit=temp,
                value=5772.0,
                upper_error=50.0,
                lower_error=40.0,
            )
        )
        v2_db.commit()
        v2_db.refresh(target)

        assert target["effective_temperature"] == 5772.0 * u.K
        with pytest.raises(KeyError):
            target["radius"]

    def test_duplicate_kind_rejected(self, v2_db: orm.Session):
        catalog = _make_catalog(v2_db, "DUP_KIND")
        target = _make_target(v2_db, catalog, 10001)
        temp = _make_unit(v2_db, u.K, "effective_temperature")
        # A target holds at most one parameter per unit/kind
        # (unique(target_id, unit_id)).
        v2_db.add_all(
            [
                AstroParameter(
                    target=target,
                    unit=temp,
                    value=5772.0,
                    upper_error=1.0,
                    lower_error=1.0,
                ),
                AstroParameter(
                    target=target,
                    unit=temp,
                    value=6000.0,
                    upper_error=1.0,
                    lower_error=1.0,
                ),
            ]
        )
        with pytest.raises(exc.IntegrityError):
            v2_db.commit()
        v2_db.rollback()

    def test_unit_parameters_back_reference(self, v2_db: orm.Session):
        catalog = _make_catalog(v2_db, "BACKREF")
        t1 = _make_target(v2_db, catalog, 5001)
        t2 = _make_target(v2_db, catalog, 5002)
        shared = _make_unit(v2_db, u.K, "effective_temperature")
        v2_db.add_all(
            [
                AstroParameter(
                    target=t1,
                    unit=shared,
                    value=5772.0,
                    upper_error=1.0,
                    lower_error=1.0,
                ),
                AstroParameter(
                    target=t2,
                    unit=shared,
                    value=4000.0,
                    upper_error=1.0,
                    lower_error=1.0,
                ),
            ]
        )
        v2_db.commit()
        v2_db.refresh(shared)

        assert len(shared.parameters) == 2
        assert {p.target_id for p in shared.parameters} == {t1.id, t2.id}

    def test_cascade_on_target_delete(self, v2_db: orm.Session):
        catalog = _make_catalog(v2_db, "CASCADE")
        target = _make_target(v2_db, catalog, 6001)
        unit = _make_unit(v2_db, u.K, "effective_temperature")
        v2_db.add(
            AstroParameter(
                target=target,
                unit=unit,
                value=5772.0,
                upper_error=1.0,
                lower_error=1.0,
            )
        )
        v2_db.commit()

        # SQL-level delete exercises the DB FK cascade, not ORM cascade.
        v2_db.execute(delete(Target).where(Target.id == target.id))
        v2_db.commit()

        assert v2_db.execute(select(AstroParameter)).scalars().all() == []
        # The shared unit is a lookup; it must survive the target delete.
        assert v2_db.get(AstroUnit, unit.id) is not None

    def test_restrict_on_unit_delete(self, v2_db: orm.Session):
        catalog = _make_catalog(v2_db, "RESTRICT")
        target = _make_target(v2_db, catalog, 7001)
        unit = _make_unit(v2_db, u.K, "effective_temperature")
        v2_db.add(
            AstroParameter(
                target=target,
                unit=unit,
                value=5772.0,
                upper_error=1.0,
                lower_error=1.0,
            )
        )
        v2_db.commit()

        # Deleting a unit still referenced by a parameter is blocked.
        with pytest.raises(exc.IntegrityError):
            v2_db.execute(delete(AstroUnit).where(AstroUnit.id == unit.id))
            v2_db.commit()
        v2_db.rollback()
