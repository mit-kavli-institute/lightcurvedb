import pandas as pd

from lightcurvedb.models.spacecraft import SpacecraftEphemeris

from .base import Ingestor


class SpacecraftEphemerisIngestor(Ingestor):

    EmissionModel = SpacecraftEphemeris

    def parse(self, descriptor):
        csv = pd.read_csv(descriptor, comment='#')
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
