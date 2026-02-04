"""Tests for Rich formatting support on LCDB models.

All tests are pure unit tests — no database session required since we only
access model attributes set via constructor, not persisted data.
"""

import uuid

import numpy as np
from rich.console import Console

from lightcurvedb.core.base_model import LCDBModel, _format_array_summary
from lightcurvedb.models import (
    DataSet,
    DataSetHierarchy,
    FITSFrame,
    Instrument,
    Mission,
    MissionCatalog,
    Observation,
    PhotometricSource,
    ProcessingMethod,
    QualityFlagArray,
    Target,
    TargetSpecificTime,
)

# ---------------------------------------------------------------------------
# TestFormatArraySummary
# ---------------------------------------------------------------------------


class TestFormatArraySummary:
    def test_none_input(self):
        assert _format_array_summary(None) == "None"

    def test_non_array_input(self):
        assert _format_array_summary("hello") == repr("hello")

    def test_empty_array(self):
        arr = np.array([], dtype=np.int64)
        assert _format_array_summary(arr) == "[]"

    def test_small_array(self):
        arr = np.array([1, 2, 3])
        assert _format_array_summary(arr) == "[1, 2, 3]"

    def test_exactly_six_elements(self):
        arr = np.array([1, 2, 3, 4, 5, 6])
        assert _format_array_summary(arr) == "[1, 2, 3, 4, 5, 6]"

    def test_large_int_array(self):
        arr = np.arange(100, dtype=np.int64)
        result = _format_array_summary(arr)
        assert result == "int64[100] range=[0, 99]"

    def test_large_float_array(self):
        arr = np.linspace(0, 1, 10)
        result = _format_array_summary(arr)
        assert "float64[10]" in result
        assert "range=" in result
        assert "0.0" in result
        assert "1.0" in result


# ---------------------------------------------------------------------------
# TestRichReprOverrides
# ---------------------------------------------------------------------------


class TestRichReprOverrides:
    def test_mission(self):
        mid = uuid.uuid4()
        m = Mission(
            id=mid,
            name="TESS",
            description="Test mission",
            time_unit="day",
            time_epoch=2457000,
            time_epoch_scale="tdb",
            time_epoch_format="jd",
            time_format_name="btjd",
        )
        pairs = list(m.__rich_repr__())
        assert pairs == [("id", mid), ("name", "TESS")]

    def test_mission_catalog(self):
        mid = uuid.uuid4()
        mc = MissionCatalog(
            id=1,
            name="TIC",
            description="TESS Input Catalog",
            host_mission_id=mid,
        )
        pairs = list(mc.__rich_repr__())
        assert pairs == [("id", 1), ("name", "TIC"), ("mission", mid)]

    def test_target(self):
        t = Target(id=42, catalog_id=1, name=261136679)
        pairs = list(t.__rich_repr__())
        assert pairs == [("id", 42), ("catalog", 1), ("name", 261136679)]

    def test_observation(self):
        inst_id = uuid.uuid4()
        o = Observation(
            id=10,
            type="observation",
            cadence_reference=np.arange(10, dtype=np.int64),
            instrument_id=inst_id,
        )
        pairs = list(o.__rich_repr__())
        assert pairs == [
            ("id", 10),
            ("type", "observation"),
            ("instrument", inst_id),
        ]

    def test_target_specific_time(self):
        tst = TargetSpecificTime(
            observation_id=1,
            target_id=42,
            barycentric_julian_dates=np.array([1.0, 2.0]),
        )
        pairs = list(tst.__rich_repr__())
        assert pairs == [("observation_id", 1), ("target_id", 42)]

    def test_instrument_without_parent(self):
        inst_id = uuid.uuid4()
        inst = Instrument(
            id=inst_id, name="Camera 1", properties={}, parent_id=None
        )
        pairs = list(inst.__rich_repr__())
        assert len(pairs) == 2
        assert pairs == [("id", inst_id), ("name", "Camera 1")]

    def test_instrument_with_parent(self):
        inst_id = uuid.uuid4()
        parent_id = uuid.uuid4()
        inst = Instrument(
            id=inst_id, name="Camera 1", properties={}, parent_id=parent_id
        )
        pairs = list(inst.__rich_repr__())
        assert len(pairs) == 3
        assert pairs == [
            ("id", inst_id),
            ("name", "Camera 1"),
            ("parent", parent_id),
        ]

    def test_quality_flag_array_without_target(self):
        qfa = QualityFlagArray(
            id=5,
            type="base_quality_flag",
            observation_id=1,
            target_id=None,
            quality_flags=np.array([0, 1], dtype=np.int32),
        )
        pairs = list(qfa.__rich_repr__())
        assert len(pairs) == 3
        assert pairs == [
            ("id", 5),
            ("type", "base_quality_flag"),
            ("obs", 1),
        ]

    def test_quality_flag_array_with_target(self):
        qfa = QualityFlagArray(
            id=5,
            type="base_quality_flag",
            observation_id=1,
            target_id=42,
            quality_flags=np.array([0, 1], dtype=np.int32),
        )
        pairs = list(qfa.__rich_repr__())
        assert len(pairs) == 4
        assert pairs == [
            ("id", 5),
            ("type", "base_quality_flag"),
            ("obs", 1),
            ("target", 42),
        ]

    def test_fits_frame(self):
        ff = FITSFrame(
            id=100,
            type="basefits",
            cadence=12345,
            observation_id=1,
            simple=True,
            bitpix=16,
            bscale=1.0,
            bzero=0.0,
            naxis=2,
            naxis_values=[2048, 2048],
            extended=True,
        )
        pairs = list(ff.__rich_repr__())
        assert pairs == [
            ("id", 100),
            ("type", "basefits"),
            ("cadence", 12345),
            ("obs", 1),
        ]

    def test_photometric_source(self):
        ps = PhotometricSource(
            id=0, name="unspecified", description="No source specified"
        )
        pairs = list(ps.__rich_repr__())
        assert pairs == [("id", 0), ("name", "unspecified")]

    def test_processing_method(self):
        pm = ProcessingMethod(
            id=1, name="detrend-v1", description="Detrending v1"
        )
        pairs = list(pm.__rich_repr__())
        assert pairs == [("id", 1), ("name", "detrend-v1")]

    def test_dataset_hierarchy(self):
        dsh = DataSetHierarchy(
            source_observation_id=1,
            source_target_id=10,
            source_photometric_method_id=0,
            source_processing_method_id=0,
            child_observation_id=2,
            child_target_id=20,
            child_photometric_method_id=0,
            child_processing_method_id=0,
        )
        pairs = list(dsh.__rich_repr__())
        assert pairs == [
            ("source_obs", 1),
            ("source_target", 10),
            ("child_obs", 2),
            ("child_target", 20),
        ]

    def test_dataset(self):
        ds = DataSet(
            observation_id=1,
            target_id=42,
            photometric_method_id=0,
            processing_method_id=0,
            values=np.array([1.0, 2.0]),
        )
        pairs = list(ds.__rich_repr__())
        assert pairs == [
            ("obs", 1),
            ("target", 42),
            ("phot", 0),
            ("proc", 0),
        ]


# ---------------------------------------------------------------------------
# TestRichConsoleProtocol
# ---------------------------------------------------------------------------


class TestRichConsoleProtocol:
    def _render(self, model) -> str:
        console = Console(width=120, force_terminal=True)
        with console.capture() as capture:
            console.print(model)
        return capture.get()

    def test_photometric_source_console_output(self):
        ps = PhotometricSource(
            id=1, name="aperture", description="Simple aperture photometry"
        )
        output = self._render(ps)
        assert "PhotometricSource" in output
        assert "id" in output
        assert "name" in output

    def test_dataset_console_output_with_arrays(self):
        ds = DataSet(
            observation_id=1,
            target_id=42,
            photometric_method_id=0,
            processing_method_id=0,
            values=np.arange(100, dtype=np.float64),
            errors=None,
        )
        output = self._render(ds)
        assert "DataSet" in output
        assert "observation_id" in output
        assert "values" in output
        # Array should be summarized, not printed in full
        assert "float64[100]" in output


# ---------------------------------------------------------------------------
# TestDefaultRichRepr
# ---------------------------------------------------------------------------


class TestDefaultRichRepr:
    def test_base_rich_repr_yields_primary_keys(self):
        """The base LCDBModel.__rich_repr__ yields PK columns."""
        ps = PhotometricSource(id=99, name="test", description="Test source")
        # Call the base class method directly, bypassing the override
        pairs = list(LCDBModel.__rich_repr__(ps))
        # PhotometricSource has a single PK column: id
        assert ("id", 99) in pairs
        # The base method should not yield non-PK columns
        keys = [k for k, _ in pairs]
        assert "name" not in keys
