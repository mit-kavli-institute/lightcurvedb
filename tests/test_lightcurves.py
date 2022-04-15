import pathlib
from tempfile import TemporaryDirectory
from hypothesis import given, strategies as st
from lightcurvedb.core.ingestors.lightcurves import h5_to_numpy
from .strategies import tess as tess_st, ingestion as ing_st

from h5py import File as H5File


def _write_basic_info(h5, cadence, bjd, x, y):
    lcground = h5["LightCurve"]
    lcground.create_dataset("Cadence", data=cadence)
    lcground.create_dataset("BJD", data=bjd)
    lcground.create_dataset("X", data=x)
    lcground.create_dataset("Y", data=y)
    lcground.create_group("AperturePhotometry")


def _write_aperture_photometry(h5, lightcurve, data):
    ap = h5["LightCurve"]["AperturePhotometry"]
    ap.create_group(
        lightcurve["aperture_id"]
    )


def _simulate_h5(data, directory):
    tic_id = data.draw(tess_st())
    lightcurves = data.draw(
        st.lists(
            ing_st.lightcurves(tic_id=st.just(tic_id)),
            min_size=1
        )
    )
    filename = pathlib.Path(f"{tic_id}.h5")
    with H5File(directory / filename, "w") as h5:
        for lightcurve in lightcurves:
            raise NotImplementedError
