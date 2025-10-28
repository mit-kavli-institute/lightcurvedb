import numpy as np
import sqlalchemy as sa
from sqlalchemy import orm

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


class TestQLPLightcurveHierarchies:
    def test_qlp_lightcurve_hierarchy(self, v2_db: orm.Session):
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

        # Add all apertures
        legacy_apertures = [
            PhotometricSource(
                name=f"Aperture_{i:03d}",  # noqa: E231
                description="Older FiPhot based SAP",
            )
            for i in range(5)
        ]
        legacy_apertures.append(
            PhotometricSource(
                name="Background Aperture",
                description="A ringed aperture for extracting background flux",
            )
        )
        v2_db.add_all(legacy_apertures)
        # Add TGLC apertures
        tglc_apertures: list[PhotometricSource] = []
        tglc_apertures.append(
            PhotometricSource(
                name="TGLC Small Aperture",
                description="A 1x1 Single Pixel Aperture from TGLC",
            )
        )
        tglc_apertures.append(
            PhotometricSource(
                name="TGLC Primary Aperture",
                description="A 3x3 Pixel Aperture from TGLC",
            )
        )
        tglc_apertures.append(
            PhotometricSource(
                name="TGLC Large Aperture",
                description="A 5x5 Aperture from TGLC",
            )
        )

        v2_db.add_all(tglc_apertures)

        # Add Detrending Types
        processing_methods = [
            ProcessingMethod(
                name="QSPIntermediateMagnitude",
                description="Systematic Corrected Lightcurves",
            ),
            ProcessingMethod(
                name="QSPMagnitude",
                description="Stellar and Quaternion based detrending",
            ),
            ProcessingMethod(
                name="KSPMagnitude", description="Kepler Spline detrending"
            ),
        ]
        v2_db.add_all(processing_methods)
        v2_db.flush()

        # Add spatial types
        spatial_methods = [
            ProcessingMethod(
                name="FFI X Position", description="X Centroid Information"
            ),
            ProcessingMethod(
                name="FFI Y Position", description="Y Centroid Information"
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
                processing_method=None,  # No processing has been done...
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
            raw_photometry.derived_datasets.append(ksp)
            raw_photometry.derived_datasets.append(sys_removed)
            sys_removed.derived_datasets.append(detrended)

            v2_db.commit()
