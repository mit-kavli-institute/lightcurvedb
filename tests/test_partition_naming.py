"""Property and example tests for partition naming.

Pure -- no database. The central property is that ``parse`` and ``table``
are exact inverses over the canonical form, so a name that comes out of
``pg_catalog`` maps back to the ``PartitionName`` that produced it.
"""

import dataclasses

import pytest
from hypothesis import given
from hypothesis import strategies as st

from lightcurvedb.core.partitions import (
    MAX_IDENTIFIER_BYTES,
    OBJECT_SUFFIXES,
    PartitionError,
    PartitionName,
    PartitionNameError,
    PartitionNameTooLongError,
)

from .strategies import partitioning as part_st


class TestRoundTrip:
    @given(part_st.partition_names())
    def test_parse_inverts_table(self, name: PartitionName):
        assert PartitionName.parse(name.table) == name

    @given(part_st.partition_names())
    def test_table_inverts_parse(self, name: PartitionName):
        rendered = name.table
        assert PartitionName.parse(rendered).table == rendered

    @given(part_st.partition_names())
    def test_try_parse_agrees_with_parse(self, name: PartitionName):
        assert PartitionName.try_parse(name.table) == name

    @given(part_st.partition_names())
    def test_str_is_table(self, name: PartitionName):
        assert str(name) == name.table


class TestRendering:
    def test_revision_zero_is_the_legacy_form(self):
        """Matches the ``dataset_obs_1`` convention in the CHANGELOG, so
        hand-made partitions and freshly provisioned ones share a name."""
        assert PartitionName("dataset", 1).table == "dataset_obs_1"
        assert PartitionName("dataset", 1, revision=0).table == "dataset_obs_1"

    def test_positive_revision_is_versioned(self):
        assert PartitionName("dataset", 5, 3).table == "dataset_obs_5_v3"

    @given(part_st.partition_names(), part_st.object_suffixes())
    def test_derived_is_table_plus_suffix(
        self, name: PartitionName, suffix: str
    ):
        assert name.derived(suffix) == f"{name.table}_{suffix}"

    def test_documented_worst_case_fits(self):
        """The requirements doc computes this as 46 bytes; pin it."""
        name = PartitionName("datasethierarchy", 2147483647, 999)
        worst = name.derived("child_idx")
        assert worst == "datasethierarchy_obs_2147483647_v999_child_idx"
        assert len(worst.encode()) == 46 <= MAX_IDENTIFIER_BYTES

    def test_is_frozen(self):
        name = PartitionName("dataset", 1)
        with pytest.raises(dataclasses.FrozenInstanceError):
            name.revision = 2  # type: ignore[misc]


class TestIdentifierLimit:
    @given(part_st.partition_names(), part_st.object_suffixes())
    def test_every_documented_object_fits(
        self, name: PartitionName, suffix: str
    ):
        assert len(name.derived(suffix).encode()) <= MAX_IDENTIFIER_BYTES

    def test_construction_rejects_names_that_cannot_own_their_objects(self):
        """A name that constructs must be able to name every object.

        ``base`` of 45 chars + ``_obs_`` + 10 digits + ``_target_idx``
        pushes the longest documented object past 63 bytes.
        """
        with pytest.raises(PartitionNameTooLongError):
            PartitionName("a" * 45, 2147483647)

    def test_derived_rejects_overlong_suffix(self):
        name = PartitionName("dataset", 1)
        with pytest.raises(PartitionNameTooLongError):
            name.derived("s" * 60)

    def test_too_long_is_a_name_error_and_a_partition_error(self):
        """Callers may catch at either level of the hierarchy."""
        assert issubclass(PartitionNameTooLongError, PartitionNameError)
        assert issubclass(PartitionNameError, PartitionError)
        assert issubclass(PartitionNameError, ValueError)


class TestParsingRejectsNonCanonical:
    @pytest.mark.parametrize(
        "name",
        [
            "dataset",  # no key
            "dataset_obs_",  # empty key
            "dataset_obs_x",  # non-numeric key
            "dataset_obs_05",  # leading zero would not round-trip
            "dataset_obs_5_v0",  # revision 0 is spelled by omission
            "dataset_obs_5_v03",  # leading zero in revision
            "dataset_obs_5_v",  # empty revision
            "dataset_obs_5_r3",  # wrong revision prefix
            "dataset_obs_5_v3_pkey",  # derived object, not the table
            "Dataset_obs_5",  # uppercase would need quoting
            "dataset_obs_5 ",  # trailing whitespace
            "",
        ],
    )
    def test_rejected(self, name: str):
        with pytest.raises(PartitionNameError):
            PartitionName.parse(name)
        assert PartitionName.try_parse(name) is None

    @given(st.text(max_size=80))
    def test_try_parse_never_raises(self, text: str):
        result = PartitionName.try_parse(text)
        if result is not None:
            assert result.table == text


class TestConstructionValidation:
    @pytest.mark.parametrize(
        "base", ["Dataset", "data-set", "1dataset", "", "data set"]
    )
    def test_base_must_be_lowercase_identifier(self, base: str):
        with pytest.raises(PartitionNameError):
            PartitionName(base, 1)

    def test_negative_ids_rejected(self):
        with pytest.raises(PartitionNameError):
            PartitionName("dataset", -1)
        with pytest.raises(PartitionNameError):
            PartitionName("dataset", 1, revision=-1)

    @pytest.mark.parametrize("suffix", ["Pkey", "_x", "1x", "", "a-b"])
    def test_suffix_must_be_identifier_fragment(self, suffix: str):
        with pytest.raises(PartitionNameError):
            PartitionName("dataset", 1).derived(suffix)


def test_object_suffixes_match_requirements_doc():
    """Section 9.3 lists exactly these five."""
    assert set(OBJECT_SUFFIXES) == {
        "pkey",
        "target_idx",
        "src_idx",
        "child_idx",
        "partcheck",
    }
