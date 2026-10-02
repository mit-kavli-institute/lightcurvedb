"""Hypothesis strategies for partition naming.

Observation ids reuse :func:`tests.strategies.tess.orbits` so generated
partition keys look like the values the real system will see, alongside
the full non-negative int4 range to exercise the identifier-length limit.
"""

from hypothesis import strategies as st

from lightcurvedb.core.partitions import OBJECT_SUFFIXES, PartitionName

from . import tess

#: The two parents partitioned today, plus generated identifiers so the
#: tests do not quietly depend on the current schema.
KNOWN_BASES = ("dataset", "datasethierarchy", "target_specific_time")


def bases():
    generated = st.from_regex(r"\A[a-z][a-z0-9_]{0,20}\Z", fullmatch=True)
    return st.one_of(st.sampled_from(KNOWN_BASES), generated)


def observation_ids():
    return st.one_of(
        tess.orbits(), st.integers(min_value=0, max_value=2**31 - 1)
    )


def revisions():
    # Revision 0 (the legacy form) is over-represented on purpose: it is
    # the one that renders differently.
    return st.one_of(st.just(0), st.integers(min_value=0, max_value=10**6))


def object_suffixes():
    return st.sampled_from(OBJECT_SUFFIXES)


@st.composite
def partition_names(draw):
    """A constructible :class:`PartitionName`.

    Filters out the rare combinations whose derived names would exceed the
    identifier limit; those are exercised explicitly rather than by chance.
    """
    base = draw(bases())
    observation_id = draw(observation_ids())
    revision = draw(revisions())
    try:
        return PartitionName(base, observation_id, revision)
    except ValueError:
        return draw(partition_names())
