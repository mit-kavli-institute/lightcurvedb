import pandas as pd
from astropy.time import Time

from .base import Ingestor
from lightcurvedb.models.spacecraft import SpacecraftEphemeris


class SpacecraftEphemerisIngestor(Ingestor):

    EmissionModel = SpacecraftEphemeris

    def parse(self, descriptor):
        csv = pd.read_csv(descriptor, comment='#')
        #tdb = Time(csv['JDTDB'], format='jd')
        for row in csv.iterrows():
            yield {
                'barycentric_dynamical_time': row['JDTDB'],
                'x_coordinate': row['X'],
                'y_coordinate': row['Y'],
                'z_coordinate': row['Z'],
                'light_travel_time': row['LT'],
                'range_to': row['RG'],
                'range_rate': row['RR']
            }