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
from sqlalchemy import orm, select

from lightcurvedb.models import AstroUnit

from .strategies import astro as astro_st


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
    """Scaffold for the next round.

    Design decision (resolved): scale/unit *correctness* is a developer
    contract. The persistence layer does NOT convert or normalize -- it stores
    ``value`` together with the associated unit and returns them unchanged.
    Whatever scale a developer puts in is what they get back; keeping ``value``
    consistent with its ``AstroUnit`` is on them. So the tests below assert
    faithful round-trip, never normalization.

    Still open (blocks meaningful tests): how a parameter is identified. There
    is no name/type field, yet ``unique(target_id, unit_id)`` forbids two
    same-unit parameters on one target (e.g. a period and a timescale both in
    ``day``). Likely needs a ``name``/``parameter_type`` column, making the
    constraint ``(target_id, name)`` or ``(target_id, name, unit_id)``.
    """

    @pytest.mark.skip(
        reason="AstroParameter identity field undecided -- see class docstring"
    )
    def test_quantity_roundtrip_value_and_unit(self, v2_db: orm.Session):
        # Persist a parameter (value + AstroUnit); read it back and assert
        # fetched.value * fetched.unit.as_unit() == the stored value*unit,
        # exactly -- faithful persistence, no scale normalization.
        raise NotImplementedError

    @pytest.mark.skip(
        reason="AstroParameter identity field undecided -- see class docstring"
    )
    def test_asymmetric_errors_preserved(self, v2_db: orm.Session):
        # upper_error and lower_error survive independently (value -lo +hi).
        raise NotImplementedError

    @pytest.mark.skip(
        reason="AstroParameter identity field undecided -- see class docstring"
    )
    def test_value_and_unit_stored_verbatim(self, v2_db: orm.Session):
        # Developer-contract scale: a value paired with a scaled/log unit is
        # returned exactly as written; the layer performs no conversion.
        raise NotImplementedError
