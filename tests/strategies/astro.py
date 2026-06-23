"""Hypothesis strategies for astropy units and :class:`AstroUnit` models.

The strategies draw real units defined in :mod:`astropy.units` -- both named
units (``m``, ``km``, ``mag`` ...) and composites built from them (``m / s``,
``kg m2`` ...) -- so tests can prove that the string serialization used by
:class:`~lightcurvedb.models.AstroUnit` survives a round-trip through the
database.
"""

import astropy.units as u
from astropy.units import UnitBase
from hypothesis import strategies as st

from lightcurvedb.models import AstroUnit


def is_roundtrippable(unit: UnitBase) -> bool:
    """Whether ``u.Unit(str(unit))`` reproduces ``unit``.

    ``UnitBase.__eq__`` compares physical decomposition *and* scale, so this
    rejects any unit whose string form loses dimension or scale. It is a safety
    net rather than a heavy filter: on astropy 8.0.0 every named unit passes.
    """
    try:
        return u.Unit(str(unit)) == unit
    except Exception:
        return False


def _collect_named_units() -> list[UnitBase]:
    """All round-trippable units exposed as attributes of :mod:`astropy.units`.

    Deduped by serialized form so the ~4000 prefixed units (``km``, ``mm`` ...)
    don't swamp ``sampled_from``'s shrinking toward the first element.
    """
    seen: dict[str, UnitBase] = {}
    for name in dir(u):
        if name.startswith("_"):
            continue
        obj = getattr(u, name, None)
        if isinstance(obj, UnitBase) and is_roundtrippable(obj):
            seen.setdefault(str(obj), obj)
    return list(seen.values())


_NAMED_UNITS = _collect_named_units()
assert _NAMED_UNITS, "no astropy units collected; sampled_from([]) would raise"


def astropy_named_units():
    """Draw a single named astropy unit (round-trippable by construction)."""
    return st.sampled_from(_NAMED_UNITS)


def _safe_pow(args):
    unit, exponent = args
    return unit**exponent


def astropy_composite_units(max_leaves: int = 5):
    """Draw a composite unit from ``*``, ``/`` and integer powers.

    Powers range over ``[-3, 3] \\ {0}``. Filtered to round-trippable units so
    consumers can assert ``u.Unit(str(unit)) == unit`` directly.

    The named pool includes extreme-magnitude units (``foe``, ``Bol``,
    quetta/exa prefixes). Powered and multiplied, these reach scales near the
    float64 ceiling (e.g. ``foe**6`` ~ 1e306), where astropy's own
    decomposition overflows or loses precision and the round-trip ``==`` stops
    holding. ``is_roundtrippable`` drops exactly those; that is an astropy
    numeric limit, not an ``AstroUnit`` defect, and out of scope for the
    developer-maintained units this models.
    """
    base = astropy_named_units()
    # Exclude exponent 0: ``unit ** 0`` collapses to dimensionless. Bound the
    # range so composite strings stay short and cheap to re-parse.
    powers = st.integers(min_value=-3, max_value=3).filter(lambda e: e != 0)

    def extend(children):
        powered = st.tuples(children, powers).map(_safe_pow)
        binop = st.tuples(children, children).flatmap(
            lambda pair: st.sampled_from(
                [pair[0] * pair[1], pair[0] / pair[1]]
            )
        )
        return st.one_of(powered, binop)

    tree = st.recursive(base, extend, max_leaves=max_leaves)
    # Drop dimensionless via ``len(x.bases)`` not ``x != dimensionless``:
    # comparing units decomposes to base SI and overflows float64 for extreme
    # composites, raising during generation. ``.bases`` is a plain attribute.
    return tree.filter(lambda x: len(x.bases) > 0 and is_roundtrippable(x))


def astro_units(units=None, name=None):
    """Draw :class:`AstroUnit` instances reflected from astropy units.

    ``AstroUnit.name`` is NOT NULL and ``reflect_astropy_unit`` won't set it,
    so ``name`` defaults to the serialized unit string.
    """
    units = (
        units
        if units is not None
        else st.one_of(astropy_named_units(), astropy_composite_units())
    )
    return units.map(
        lambda unit: AstroUnit.reflect_astropy_unit(
            unit, name=name if name is not None else str(unit)
        )
    )
