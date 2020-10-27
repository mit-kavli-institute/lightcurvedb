from astropy.io import fits
from .base import MultiIngestor
from lightcurvedb.models.frame import Frame


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
