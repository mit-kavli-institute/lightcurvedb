import os

from astropy.io import fits

from lightcurvedb.models.frame import Frame

from .base import MultiIngestor


class FrameIngestor(MultiIngestor):

    EmissionModel = Frame

    POC_fits_mapper = {
        "TIME": "gps_time",
        "STARTTJD": "start_tjd",
        "MIDTJD": "mid_tjd",
        "ENDTJD": "end_tjd",
        "EXPTIME": "exp_time",
        "QUAL_BIT": "quality_bit",
        "CAM": "camera",
        "CADENCE": "cadence",
    }

    def parse(self, descriptor):
        frame_kwargs = {}
        frame_kwargs["ccd"] = self.context.get("ccd", None)
        frame_kwargs["cadence_type"] = self.context["cadence_type"]
        frame_kwargs["file_path"] = descriptor

        with fits.open(descriptor) as filein:
            header = filein[0].header

            for key, mapped_key in self.POC_fits_mapper.items():
                frame_kwargs[mapped_key] = header[key]

        yield frame_kwargs


def from_fits(path, cadence_type=30, frame_type=None, orbit=None):
    """
    Generates a Frame instance from a FITS file.
    Parameters
    ----------
    path : str or pathlike
        The path to the FITS file.
    cadence_type : int, optional
        The cadence type of the FITS file.
    frame_type : FrameType, optional
        The FrameType relation for this Frame instance, by default this
        is not set (None).
    orbit : Orbit
        The orbit this Frame was observed in. By default this is not set
        (None).

    Returns
    -------
    Frame
        The constructed frame.
    """
    abspath = os.path.abspath(path)
    header = fits.open(abspath)[0].header
    if cadence_type is None:
        cadence_type = header["INT_TIME"] // 60
    try:
        return Frame(
            cadence_type=cadence_type,
            camera=header.get("CAM", header.get("CAMNUM", None)),
            ccd=header.get("CCD", header.get("CCDNUM", None)),
            cadence=header["CADENCE"],
            gps_time=header["TIME"],
            start_tjd=header["STARTTJD"],
            mid_tjd=header["MIDTJD"],
            end_tjd=header["ENDTJD"],
            exp_time=header["EXPTIME"],
            quality_bit=header["QUAL_BIT"],
            file_path=abspath,
            frame_type=frame_type,
            orbit=orbit,
        )
    except KeyError as e:
        print(e)
        print("==={0} HEADER===".format(abspath))
        print(repr(header))
        raise
