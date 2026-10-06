import numpy as np
import pytest
import sqlalchemy as sa
from sqlalchemy import orm
from sqlalchemy.exc import IntegrityError

from lightcurvedb.core.base_model import LCDBModel
from lightcurvedb.models import (
    Instrument,
    Observation,
    PhotometricSource,
    ProcessingMethod,
)
from lightcurvedb.models.dataset import DataSet
from lightcurvedb.models.observation import TargetSpecificTime
from lightcurvedb.models.target import Mission, MissionCatalog, Target

# -----------------------------------------------------------------------------
# Module-local models
#
# The sample_* object graph these tests use now lives in conftest.py.
# -----------------------------------------------------------------------------


class Orbit(LCDBModel):
    __tablename__ = "orbit"
    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    sector: orm.Mapped[int] = orm.mapped_column(index=True)
    orbit_number: orm.Mapped[int] = orm.mapped_column(unique=True, index=True)


class TESSOrbitObservation(Observation):
    __tablename__ = "tess_orbit_observation"
    __mapper_args__ = {
        "polymorphic_identity": "tess_orbit_observation",
    }

    id: orm.Mapped[int] = orm.mapped_column(
        sa.ForeignKey("observation.id"), primary_key=True
    )
    orbit_id: orm.Mapped[int] = orm.mapped_column(
        sa.ForeignKey("orbit.id"), index=True
    )


class TestQLPLightcurveProducts:
    def test_qlp_lightcurve_products(self, v2_db: orm.Session):
        mission = Mission(
            name="TESS",
            description="A mission launched in 2018...",
            time_unit="day",
            time_epoch=2457000,
            time_epoch_scale="tdb",
            time_epoch_format="jd",
            time_format_name="btjd",
        )

        v2_db.add(mission)
        v2_db.flush()
        tic = MissionCatalog(
            name="TESS Input Catalog",
            description=(
                "A stellar catalog describing parameters for the TESS Mission."
            ),
            host_mission=mission,
        )
        v2_db.add(tic)
        v2_db.flush()

        camera_1 = Instrument(
            name="TESS Camera 1",
            properties={"type": "camera", "manufacturer": "MIT"},
        )
        v2_db.add(camera_1)
        v2_db.flush()

        ccd_1 = Instrument(
            name="Cam1 CCD1", properties={"type": "CCD"}, parent=camera_1
        )

        v2_db.add(ccd_1)
        v2_db.flush()

        # Add all apertures (explicit IDs since autoincrement=False)
        legacy_apertures = [
            PhotometricSource(
                id=i + 1,  # Start from 1, 0 is reserved for sentinel
                name=f"Aperture_{i:03d}",  # noqa: E231
                description="Older FiPhot based SAP",
            )
            for i in range(5)
        ]
        legacy_apertures.append(
            PhotometricSource(
                id=6,
                name="Background Aperture",
                description="A ringed aperture for extracting background flux",
            )
        )
        v2_db.add_all(legacy_apertures)
        # Add TGLC apertures (explicit IDs since autoincrement=False)
        tglc_apertures: list[PhotometricSource] = []
        tglc_apertures.append(
            PhotometricSource(
                id=7,
                name="TGLC Small Aperture",
                description="A 1x1 Single Pixel Aperture from TGLC",
            )
        )
        tglc_apertures.append(
            PhotometricSource(
                id=8,
                name="TGLC Primary Aperture",
                description="A 3x3 Pixel Aperture from TGLC",
            )
        )
        tglc_apertures.append(
            PhotometricSource(
                id=9,
                name="TGLC Large Aperture",
                description="A 5x5 Aperture from TGLC",
            )
        )

        v2_db.add_all(tglc_apertures)

        # Add Detrending Types (explicit IDs since autoincrement=False)
        processing_methods = [
            ProcessingMethod(
                id=1,
                name="QSPIntermediateMagnitude",
                description="Systematic Corrected Lightcurves",
            ),
            ProcessingMethod(
                id=2,
                name="QSPMagnitude",
                description="Stellar and Quaternion based detrending",
            ),
            ProcessingMethod(
                id=3,
                name="KSPMagnitude",
                description="Kepler Spline detrending",
            ),
        ]
        v2_db.add_all(processing_methods)
        v2_db.flush()

        # Add spatial types (explicit IDs since autoincrement=False)
        spatial_methods = [
            ProcessingMethod(
                id=4,
                name="FFI X Position",
                description="X Centroid Information",
            ),
            ProcessingMethod(
                id=5,
                name="FFI Y Position",
                description="Y Centroid Information",
            ),
        ]
        v2_db.add_all(spatial_methods)
        v2_db.flush()

        # Simulate orbit
        LCDBModel.metadata.create_all(bind=v2_db.connection().engine)
        orbit = Orbit(sector=1, orbit_number=9)
        v2_db.add(orbit)
        v2_db.flush()

        assert orbit.id is not None

        # Simulate target
        t = Target(catalog=tic, name=1234567890)

        # Common Observation
        cadences = np.array(list(range(100)))
        observation = TESSOrbitObservation(
            instrument=ccd_1, cadence_reference=cadences, orbit_id=orbit.id
        )
        btjd = TargetSpecificTime(
            target=t,
            observation=observation,
            barycentric_julian_dates=cadences * 200.0,
        )
        v2_db.add(btjd)
        v2_db.flush()

        # Simulate raw photometry from all previously defined apertures
        for source in legacy_apertures + tglc_apertures:
            raw_photometry = DataSet(
                values=np.random.normal(0, 1, size=cadences.shape),
                errors=np.random.normal(0, 0.1, size=cadences.shape),
                target=t,
                observation=observation,
                photometry_source=source,
                processing_method_id=ProcessingMethod.UNSPECIFIED_ID,
            )
            # Add XY positions
            x_coordinates = DataSet(
                values=np.random.normal(10, 1, size=cadences.shape),
                errors=np.random.normal(0, 0.1, size=cadences.shape),
                target=t,
                observation=observation,
                photometry_source=source,
                processing_method=spatial_methods[0],
            )
            y_coordinates = DataSet(
                values=np.random.normal(10, 1, size=cadences.shape),
                errors=np.random.normal(0, 0.1, size=cadences.shape),
                target=t,
                observation=observation,
                photometry_source=source,
                processing_method=spatial_methods[1],
            )

            v2_db.add_all([raw_photometry, x_coordinates, y_coordinates])
            v2_db.flush()

            # Add detrending methods
            # First try legacy detrending
            ksp = DataSet(
                values=np.random.normal(0, 1, size=cadences.shape),
                errors=np.random.normal(0, 1, size=cadences.shape),
                target=t,
                observation=observation,
                photometry_source=source,
                processing_method=processing_methods[2],
            )

            sys_removed = DataSet(
                values=np.random.normal(0, 1, size=cadences.shape),
                errors=np.random.normal(0, 1, size=cadences.shape),
                target=t,
                observation=observation,
                photometry_source=source,
                processing_method=processing_methods[0],
            )
            detrended = DataSet(
                values=np.random.normal(0, 1, size=cadences.shape),
                errors=np.random.normal(0, 1, size=cadences.shape),
                target=t,
                observation=observation,
                photometry_source=source,
                processing_method=processing_methods[1],
            )
            v2_db.add_all([ksp, sys_removed, detrended])
            v2_db.commit()


# -----------------------------------------------------------------------------
# Relationship Retrieval Tests
# -----------------------------------------------------------------------------


class TestDataSetRetrieval:
    """Tests for DataSet retrieval through related models."""

    def test_target_datasets_retrieval(
        self,
        v2_db: orm.Session,
        sample_target: Target,
        sample_observation: Observation,
    ):
        """Test accessing datasets via target.datasets relationship."""
        # Create multiple photometric sources
        sources = [
            PhotometricSource(
                id=200 + i, name=f"Src{i}", description=f"Source {i}"
            )
            for i in range(3)
        ]
        v2_db.add_all(sources)
        v2_db.flush()

        # Create datasets for the target
        datasets = []
        for source in sources:
            ds = DataSet(
                values=np.random.normal(0, 1, 100),
                target=sample_target,
                observation=sample_observation,
                photometry_source=source,
                processing_method_id=ProcessingMethod.UNSPECIFIED_ID,
            )
            datasets.append(ds)
            v2_db.add(ds)
        v2_db.flush()
        v2_db.commit()

        # Refresh target
        v2_db.refresh(sample_target)

        # Verify retrieval through target
        assert len(sample_target.datasets) == 3
        for ds in datasets:
            assert ds in sample_target.datasets

    def test_observation_datasets_retrieval(
        self,
        v2_db: orm.Session,
        sample_target: Target,
        sample_observation: Observation,
    ):
        """Test accessing datasets via observation.datasets relationship."""
        # Create multiple photometric sources
        sources = [
            PhotometricSource(
                id=210 + i, name=f"ObsSrc{i}", description=f"Src {i}"
            )
            for i in range(3)
        ]
        v2_db.add_all(sources)
        v2_db.flush()

        # Create datasets for the observation
        datasets = []
        for source in sources:
            ds = DataSet(
                values=np.random.normal(0, 1, 100),
                target=sample_target,
                observation=sample_observation,
                photometry_source=source,
                processing_method_id=ProcessingMethod.UNSPECIFIED_ID,
            )
            datasets.append(ds)
            v2_db.add(ds)
        v2_db.flush()
        v2_db.commit()

        # Refresh observation
        v2_db.refresh(sample_observation)

        # Verify retrieval through observation
        assert len(sample_observation.datasets) == 3
        for ds in datasets:
            assert ds in sample_observation.datasets

    def test_photometric_source_datasets_retrieval(
        self,
        v2_db: orm.Session,
        sample_target: Target,
        sample_observation: Observation,
        sample_photometric_source: PhotometricSource,
    ):
        """Test accessing datasets via photometric_source.datasets."""
        # Create multiple processing methods
        methods = [
            ProcessingMethod(
                id=220 + i, name=f"PsMethod{i}", description=f"M {i}"
            )
            for i in range(3)
        ]
        v2_db.add_all(methods)
        v2_db.flush()

        # Create datasets with same photometric source
        datasets = []
        for method in methods:
            ds = DataSet(
                values=np.random.normal(0, 1, 100),
                target=sample_target,
                observation=sample_observation,
                photometry_source=sample_photometric_source,
                processing_method=method,
            )
            datasets.append(ds)
            v2_db.add(ds)
        v2_db.flush()
        v2_db.commit()

        # Refresh photometric source
        v2_db.refresh(sample_photometric_source)

        # Verify retrieval through photometric source
        assert len(sample_photometric_source.datasets) == 3
        for ds in datasets:
            assert ds in sample_photometric_source.datasets

    def test_processing_method_datasets_retrieval(
        self,
        v2_db: orm.Session,
        sample_target: Target,
        sample_observation: Observation,
        sample_processing_method: ProcessingMethod,
    ):
        """Test accessing datasets via processing_method.datasets."""
        # Create multiple photometric sources
        sources = [
            PhotometricSource(
                id=230 + i, name=f"PmSrc{i}", description=f"S {i}"
            )
            for i in range(3)
        ]
        v2_db.add_all(sources)
        v2_db.flush()

        # Create datasets with same processing method
        datasets = []
        for source in sources:
            ds = DataSet(
                values=np.random.normal(0, 1, 100),
                target=sample_target,
                observation=sample_observation,
                photometry_source=source,
                processing_method=sample_processing_method,
            )
            datasets.append(ds)
            v2_db.add(ds)
        v2_db.flush()
        v2_db.commit()

        # Refresh processing method
        v2_db.refresh(sample_processing_method)

        # Verify retrieval through processing method
        assert len(sample_processing_method.datasets) == 3
        for ds in datasets:
            assert ds in sample_processing_method.datasets


# -----------------------------------------------------------------------------
# Hybrid Property Filtering Tests
# -----------------------------------------------------------------------------


class TestHybridPropertyFiltering:
    """Tests for DataSet hybrid property SQL filtering."""

    def test_filter_has_photometric_source(
        self,
        v2_db: orm.Session,
        sample_target: Target,
        sample_observation: Observation,
        sample_photometric_source: PhotometricSource,
    ):
        """Test filtering datasets with photometric source via SQL."""
        # Create dataset with photometric source (not sentinel)
        with_source = DataSet(
            values=np.random.normal(0, 1, 100),
            target=sample_target,
            observation=sample_observation,
            photometry_source=sample_photometric_source,
            processing_method_id=ProcessingMethod.UNSPECIFIED_ID,
        )
        v2_db.add(with_source)
        v2_db.flush()

        # Create dataset with sentinel (no photometric source)
        method = ProcessingMethod(id=300, name="HpTest", description="HP test")
        v2_db.add(method)
        v2_db.flush()

        without_source = DataSet(
            values=np.random.normal(0, 1, 100),
            target=sample_target,
            observation=sample_observation,
            photometric_method_id=PhotometricSource.UNSPECIFIED_ID,
            processing_method=method,
        )
        v2_db.add(without_source)
        v2_db.commit()

        # Query using hybrid property
        with_source_results = (
            v2_db.query(DataSet)
            .filter(DataSet.has_photometric_source == True)  # noqa: E712
            .filter(DataSet.target_id == sample_target.id)
            .all()
        )
        without_source_results = (
            v2_db.query(DataSet)
            .filter(DataSet.has_photometric_source == False)  # noqa: E712
            .filter(DataSet.target_id == sample_target.id)
            .all()
        )

        assert len(with_source_results) == 1
        assert with_source in with_source_results

        assert len(without_source_results) == 1
        assert without_source in without_source_results

    def test_filter_has_processing_method(
        self,
        v2_db: orm.Session,
        sample_target: Target,
        sample_observation: Observation,
        sample_processing_method: ProcessingMethod,
    ):
        """Test filtering datasets with processing method via SQL."""
        # Create photometric source
        source = PhotometricSource(
            id=310, name="HpSource", description="HP Source"
        )
        v2_db.add(source)
        v2_db.flush()

        # Create dataset with processing method (not sentinel)
        with_method = DataSet(
            values=np.random.normal(0, 1, 100),
            target=sample_target,
            observation=sample_observation,
            photometry_source=source,
            processing_method=sample_processing_method,
        )
        v2_db.add(with_method)
        v2_db.flush()

        # Create another source for the sentinel dataset
        source2 = PhotometricSource(
            id=311, name="HpSource2", description="HP Source2"
        )
        v2_db.add(source2)
        v2_db.flush()

        # Create dataset with sentinel (no processing method - raw)
        without_method = DataSet(
            values=np.random.normal(0, 1, 100),
            target=sample_target,
            observation=sample_observation,
            photometry_source=source2,
            processing_method_id=ProcessingMethod.UNSPECIFIED_ID,
        )
        v2_db.add(without_method)
        v2_db.commit()

        # Query using hybrid property
        with_method_results = (
            v2_db.query(DataSet)
            .filter(DataSet.has_processing_method == True)  # noqa: E712
            .filter(DataSet.target_id == sample_target.id)
            .all()
        )
        without_method_results = (
            v2_db.query(DataSet)
            .filter(DataSet.has_processing_method == False)  # noqa: E712
            .filter(DataSet.target_id == sample_target.id)
            .all()
        )

        assert len(with_method_results) == 1
        assert with_method in with_method_results

        assert len(without_method_results) == 1
        assert without_method in without_method_results

    def test_filter_combined_hybrid_properties(
        self,
        v2_db: orm.Session,
        sample_target: Target,
        sample_observation: Observation,
    ):
        """Test combining hybrid property filters."""
        # Create all sources and methods first, then flush once
        source = PhotometricSource(
            id=320, name="CombSrc", description="Comb Source"
        )
        source2 = PhotometricSource(
            id=321, name="CombSrc2", description="Src2"
        )
        method = ProcessingMethod(
            id=320, name="CombMeth", description="Comb Method"
        )
        method2 = ProcessingMethod(
            id=321, name="CombMeth2", description="Meth2"
        )
        v2_db.add_all([source, source2, method, method2])
        v2_db.flush()

        # Now create all datasets (after sources/methods are in session)
        # Dataset with both (has source AND has method)
        both = DataSet(
            values=np.random.normal(0, 1, 100),
            target=sample_target,
            observation=sample_observation,
            photometry_source=source,
            processing_method=method,
        )

        # Dataset with source only (has source, no method)
        source_only = DataSet(
            values=np.random.normal(0, 1, 100),
            target=sample_target,
            observation=sample_observation,
            photometry_source=source2,
            processing_method_id=ProcessingMethod.UNSPECIFIED_ID,
        )

        # Dataset with method only (no source, has method)
        method_only = DataSet(
            values=np.random.normal(0, 1, 100),
            target=sample_target,
            observation=sample_observation,
            photometric_method_id=PhotometricSource.UNSPECIFIED_ID,
            processing_method=method2,
        )

        # Dataset with neither (no source, no method)
        neither = DataSet(
            values=np.random.normal(0, 1, 100),
            target=sample_target,
            observation=sample_observation,
            photometric_method_id=PhotometricSource.UNSPECIFIED_ID,
            processing_method_id=ProcessingMethod.UNSPECIFIED_ID,
        )

        v2_db.add_all([both, source_only, method_only, neither])
        v2_db.commit()

        # Query: has both
        both_results = (
            v2_db.query(DataSet)
            .filter(DataSet.has_photometric_source == True)  # noqa: E712
            .filter(DataSet.has_processing_method == True)  # noqa: E712
            .filter(DataSet.target_id == sample_target.id)
            .all()
        )
        assert len(both_results) == 1
        assert both in both_results

        # Query: has source but not method
        source_no_method = (
            v2_db.query(DataSet)
            .filter(DataSet.has_photometric_source == True)  # noqa: E712
            .filter(DataSet.has_processing_method == False)  # noqa: E712
            .filter(DataSet.target_id == sample_target.id)
            .all()
        )
        assert len(source_no_method) == 1
        assert source_only in source_no_method

        # Query: has neither
        has_neither = (
            v2_db.query(DataSet)
            .filter(DataSet.has_photometric_source == False)  # noqa: E712
            .filter(DataSet.has_processing_method == False)  # noqa: E712
            .filter(DataSet.target_id == sample_target.id)
            .all()
        )
        assert len(has_neither) == 1
        assert neither in has_neither


# -----------------------------------------------------------------------------
# Cascade Delete Tests
# -----------------------------------------------------------------------------


class TestCascadeDelete:
    """Tests for DataSet cascade delete behavior."""

    def test_observation_delete_cascades_to_datasets(
        self,
        v2_db: orm.Session,
        sample_target: Target,
        sample_instrument: Instrument,
    ):
        """Test deleting observation cascades to its datasets."""
        # Create observation
        observation = Observation(
            instrument=sample_instrument,
            cadence_reference=np.arange(50),
        )
        v2_db.add(observation)
        v2_db.flush()

        obs_id = observation.id

        # Create photometric source
        source = PhotometricSource(
            id=400, name="CascSrc", description="Cascade Source"
        )
        v2_db.add(source)
        v2_db.flush()

        # Create dataset
        ds = DataSet(
            values=np.random.normal(0, 1, 50),
            target=sample_target,
            observation=observation,
            photometry_source=source,
            processing_method_id=ProcessingMethod.UNSPECIFIED_ID,
        )
        v2_db.add(ds)
        v2_db.commit()

        # Verify dataset exists
        count_before = (
            v2_db.query(DataSet)
            .filter(DataSet.observation_id == obs_id)
            .count()
        )
        assert count_before == 1

        # Delete via SQL to trigger database-level CASCADE
        # (ORM delete doesn't trigger ondelete CASCADE)
        v2_db.execute(sa.delete(Observation).where(Observation.id == obs_id))
        v2_db.commit()

        # Verify dataset was cascaded
        count_after = (
            v2_db.query(DataSet)
            .filter(DataSet.observation_id == obs_id)
            .count()
        )
        assert count_after == 0

    def test_target_delete_cascades_to_datasets(
        self,
        v2_db: orm.Session,
        sample_catalog: MissionCatalog,
        sample_observation: Observation,
    ):
        """Test deleting target cascades to its datasets."""
        # Create target
        target = Target(catalog=sample_catalog, name=999888777)
        v2_db.add(target)
        v2_db.flush()

        target_id = target.id

        # Create photometric source
        source = PhotometricSource(
            id=410, name="TgtCascSrc", description="Target Cascade"
        )
        v2_db.add(source)
        v2_db.flush()

        # Create dataset
        ds = DataSet(
            values=np.random.normal(0, 1, 100),
            target=target,
            observation=sample_observation,
            photometry_source=source,
            processing_method_id=ProcessingMethod.UNSPECIFIED_ID,
        )
        v2_db.add(ds)
        v2_db.commit()

        # Verify dataset exists
        count_before = (
            v2_db.query(DataSet).filter(DataSet.target_id == target_id).count()
        )
        assert count_before == 1

        # Delete target via SQL to trigger database-level CASCADE
        v2_db.execute(sa.delete(Target).where(Target.id == target_id))
        v2_db.commit()

        # Verify dataset was cascaded
        count_after = (
            v2_db.query(DataSet).filter(DataSet.target_id == target_id).count()
        )
        assert count_after == 0

    def test_photometric_source_delete_restricted(
        self,
        v2_db: orm.Session,
        sample_target: Target,
        sample_observation: Observation,
    ):
        """Test deleting photometric source fails when datasets exist."""
        # Create photometric source
        source = PhotometricSource(
            id=430, name="RestrictSrc", description="Restricted"
        )
        v2_db.add(source)
        v2_db.flush()

        source_id = source.id

        # Create dataset referencing the source
        ds = DataSet(
            values=np.random.normal(0, 1, 100),
            target=sample_target,
            observation=sample_observation,
            photometry_source=source,
            processing_method_id=ProcessingMethod.UNSPECIFIED_ID,
        )
        v2_db.add(ds)
        v2_db.commit()

        # Attempt to delete via SQL - should fail with RESTRICT
        # (ORM delete doesn't trigger database-level constraints)
        with pytest.raises(IntegrityError):
            v2_db.execute(
                sa.delete(PhotometricSource).where(
                    PhotometricSource.id == source_id
                )
            )
            v2_db.commit()
        v2_db.rollback()

    def test_processing_method_delete_restricted(
        self,
        v2_db: orm.Session,
        sample_target: Target,
        sample_observation: Observation,
    ):
        """Test deleting processing method fails when datasets exist."""
        # Create photometric source and processing method
        source = PhotometricSource(
            id=440, name="PmRestrictSrc", description="PM Restrict"
        )
        method = ProcessingMethod(
            id=440, name="RestrictMeth", description="Restricted"
        )
        v2_db.add_all([source, method])
        v2_db.flush()

        method_id = method.id

        # Create dataset referencing the method
        ds = DataSet(
            values=np.random.normal(0, 1, 100),
            target=sample_target,
            observation=sample_observation,
            photometry_source=source,
            processing_method=method,
        )
        v2_db.add(ds)
        v2_db.commit()

        # Attempt delete via SQL - should fail with RESTRICT
        with pytest.raises(IntegrityError):
            v2_db.execute(
                sa.delete(ProcessingMethod).where(
                    ProcessingMethod.id == method_id
                )
            )
            v2_db.commit()
        v2_db.rollback()


# -----------------------------------------------------------------------------
# Composite Key Query Tests
# -----------------------------------------------------------------------------


class TestCompositeKeyQueries:
    """Tests for DataSet composite key queries."""

    def test_query_by_full_composite_key(
        self,
        v2_db: orm.Session,
        sample_target: Target,
        sample_observation: Observation,
        sample_photometric_source: PhotometricSource,
        sample_processing_method: ProcessingMethod,
    ):
        """Test retrieving dataset by all 4 PK components."""
        # Create dataset
        ds = DataSet(
            values=np.random.normal(0, 1, 100),
            target=sample_target,
            observation=sample_observation,
            photometry_source=sample_photometric_source,
            processing_method=sample_processing_method,
        )
        v2_db.add(ds)
        v2_db.commit()

        # Query by all 4 components
        result = (
            v2_db.query(DataSet)
            .filter(
                DataSet.observation_id == sample_observation.id,
                DataSet.target_id == sample_target.id,
                DataSet.photometric_method_id == sample_photometric_source.id,
                DataSet.processing_method_id == sample_processing_method.id,
            )
            .one_or_none()
        )

        assert result is not None
        assert result.target_id == sample_target.id

    def test_query_by_observation_and_target(
        self,
        v2_db: orm.Session,
        sample_target: Target,
        sample_observation: Observation,
    ):
        """Test filtering by observation_id and target_id."""
        # Create multiple datasets for same observation/target
        sources = [
            PhotometricSource(
                id=500 + i, name=f"CkSrc{i}", description=f"CK {i}"
            )
            for i in range(3)
        ]
        v2_db.add_all(sources)
        v2_db.flush()

        for source in sources:
            ds = DataSet(
                values=np.random.normal(0, 1, 100),
                target=sample_target,
                observation=sample_observation,
                photometry_source=source,
                processing_method_id=ProcessingMethod.UNSPECIFIED_ID,
            )
            v2_db.add(ds)
        v2_db.commit()

        # Query by observation + target
        results = (
            v2_db.query(DataSet)
            .filter(
                DataSet.observation_id == sample_observation.id,
                DataSet.target_id == sample_target.id,
            )
            .all()
        )

        assert len(results) == 3

    @pytest.mark.filterwarnings(
        "ignore:New instance .* conflicts with persistent "
        "instance:sqlalchemy.exc.SAWarning"
    )
    def test_unique_constraint_composite_key(
        self,
        v2_db: orm.Session,
        sample_target: Target,
        sample_observation: Observation,
    ):
        """Test duplicate composite key raises IntegrityError."""
        # Create photometric source and processing method
        source = PhotometricSource(
            id=510, name="UniqSrc", description="Unique"
        )
        method = ProcessingMethod(
            id=510, name="UniqMeth", description="Unique"
        )
        v2_db.add_all([source, method])
        v2_db.flush()

        # Create first dataset
        ds1 = DataSet(
            values=np.random.normal(0, 1, 100),
            target=sample_target,
            observation=sample_observation,
            photometry_source=source,
            processing_method=method,
        )
        v2_db.add(ds1)
        v2_db.commit()

        # Attempt to create duplicate with same composite key
        ds2 = DataSet(
            values=np.random.normal(0, 1, 100),
            target=sample_target,
            observation=sample_observation,
            photometry_source=source,
            processing_method=method,
        )
        v2_db.add(ds2)

        with pytest.raises(IntegrityError):
            v2_db.commit()
        v2_db.rollback()
