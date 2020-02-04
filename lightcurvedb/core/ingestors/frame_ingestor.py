from astropy.io import fits
from glob import glob
from .base import MultiIngestor
from lightcurvedb.models.frame import Frame


class FrameIngestor(MultiIngestor):

    EmissionModel = Frame

    POC_fits_mapper = {
        'TIME': 'gps_time',
        'STARTTJD': 'start_tjd',
        'MIDTJD': 'mid_tdj',
        'ENDTJD': 'end_tjd',
        'EXPTIME': 'exp_time',
        'QUAL_BIT': 'quality_bit'
    }

    def parse(self, descriptor):
        frame_kwargs = {}
        frame_kwargs['camera'] = self.context['camera']
        frame_kwargs['ccd'] = self.context['ccd']
        frame_kwargs['orbit'] = self.context['orbit']
        frame_kwargs['frame_type'] = self.context['frame_type']
        frame_kwargs['file_path'] = descriptor.name

        with fits.open(descriptor) as filein:
            header = filein[0].header
            # Just some safety checks to avoid mis-assignment of orbits
            if header['ORBIT_ID'] != self.context['orbit'].orbit_number:
                raise RunTimeError(
                    'Ingested frame {} does not match given orbit {}'.format(
                        descriptor,
                        self.context['orbit']
                    )
                )

            for key, mapped_key in self.POC_fits_mapper.items():
                frame_kwargs[mapped_key] = header[key]

        yield frame_kwargs
