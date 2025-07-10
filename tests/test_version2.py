import pathlib
from itertools import product

import sqlalchemy as sa
import yaml
from sqlalchemy import orm

from lightcurvedb.core.base_model import LCDBModel
from lightcurvedb.models.frame import FITSFrame
from lightcurvedb.models.instrument import Instrument
from lightcurvedb.models.interpretation import (
    InterpretationAssociationTable,
    InterpretationType,
    ProcessingGroup,
)
from lightcurvedb.models.observation import Observation

cameras = [1, 2, 3, 4]
ccds = [1, 2, 3, 4]


def _reflect_camera(yaml_config, **kw) -> Instrument:
    return Instrument(
        **kw,
        properties={
            "serial_number": yaml_config["serial_number"],
            "angle_alpha": yaml_config["angle_alpha"],
            "angle_beta": yaml_config["angle_beta"],
            "angle_gamma": yaml_config["angle_gamma"],
            "focal_properties": yaml_config["focal_properties"],
        },
    )


def _reflect_ccd(yaml_config, **kw) -> Instrument:
    return Instrument(
        **kw,
        properties={
            "serial_number": yaml_config["serial_number"],
            "rows": yaml_config["rows"],
            "cols": yaml_config["cols"],
            "rotation": yaml_config["rotation"],
            "x_0": yaml_config["x_0"],
            "y_0": yaml_config["y_0"],
            "pixel_size_x": yaml_config["pixel_size_x"],
            "pixel_size_y": yaml_config["pixel_size_y"],
            "oc_noise": yaml_config["oc_noise"],
            "gain": yaml_config["gain"],
            "fullwell": yaml_config["fullwell"],
            "bias": yaml_config["bias"],
            "bias_stddev": yaml_config["bias_stddev"],
        },
    )


def make_tess_instrument(session: orm.Session):
    tess_yaml = yaml.safe_load(
        open(
            pathlib.Path(__name__).parent / "tess_properties.yaml", "rt"
        ).read()
    )
    sc = Instrument(
        name="TESS", description="Terrestrial Exoplanet Survey Satellite"
    )
    session.add(sc)
    session.flush()

    for cam in cameras:
        cam_spec = tess_yaml[f"camera_{cam}"]

        camera = _reflect_camera(
            cam_spec,
            name=f"Camera {cam}",
            description=f"TESS Camera #{cam}",
            parent_id=sc.id,
        )
        session.add(camera)
        session.flush()
        for ccd in ccds:
            ccd_spec = cam_spec[f"ccd_{ccd}"]

            ccd = _reflect_ccd(
                ccd_spec,
                name=f"CCD {ccd}",
                description="CCD #{ccd_spec['serial_number']}",
                parent_id=camera.id,
            )
            session.add(ccd)
            session.flush()


def make_qlp_tica_fits():
    class TICAFITS(FITSFrame):
        __tablename__ = "ticafits"
        __mapper_args__ = {"polymorphic_identity": "ticafits"}

        id: orm.Mapped[int] = orm.mapped_column(
            sa.ForeignKey(FITSFrame.id), primary_key=True
        )

        camera: orm.Mapped[int]
        ccd: orm.Mapped[int]

    return TICAFITS


def make_qlp_orbit():
    class Orbit(Observation):
        __tablename__ = "orbit"
        __mapper_args__ = {"polymorphic_identity": "orbit"}

        id: orm.Mapped[int] = orm.mapped_column(
            sa.ForeignKey(Observation.id), primary_key=True
        )
        orbit_number: orm.Mapped[int] = orm.mapped_column(unique=True)
        sector: orm.Mapped[int]

    return Orbit


def make_qlp_interpretations(session: orm.Session):
    # Ensure we can setup QLP's Apertures and Detrending types:
    possible_aperture_names = [f"Aperture_{ap: 03d}" for ap in range(5)] + [
        "TLGCApertureSmall",
        "TGLCAperturePrimary",
        "TGLCApertureLarge",
    ]

    mag_types = ["RawMagnitude", "QSPIntermediateMagnitude", "QSPMagnitude"]

    for ap, type_ in product(possible_aperture_names, mag_types):
        aperture = InterpretationType(
            name=ap, description=f"Test aperture {ap}"
        )
        mag_type = InterpretationType(
            name=type_, description=f"Lightcurve magnitude {type_}"
        )
        group = ProcessingGroup(name=f"{ap}-{type_}", description="Test group")

        session.add(aperture)
        session.add(mag_type)
        session.add(group)
        session.flush()

        session.add(
            InterpretationAssociationTable(
                group_id=group.id,
                previous_type_id=aperture.id,
                next_type_id=mag_type.id,
            )
        )
        session.flush()


def test_tess_fits_implementation(v2_db: orm.Session):
    TICAFITS = make_qlp_tica_fits()

    engine = v2_db.connection().engine

    assert not sa.inspect(engine).has_table(TICAFITS.__tablename__)
    LCDBModel.metadata.tables["ticafits"].create(bind=engine)
    assert sa.inspect(engine).has_table(TICAFITS.__tablename__)


def test_tess_qlp_interpretation(v2_db: orm.Session):
    # Ensure we can setup QLP's Apertures and Detrending types:
    make_qlp_interpretations(v2_db)
