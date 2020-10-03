from .base import PyObjIngestor
from lightcurvedb.models.orbit import Orbit
from click import echo
import re

EXTR = re.compile(r'^(?P<basename>tess[0-9]+)')


class OrbitIngestor(PyObjIngestor):

    EmissionModel = Orbit

    def check_congruence(self, headers, *fields):
        reference = headers[0]
        others = headers[1:]

        for field in fields:
            assert all(reference[field] == o[field] for o in others)

        if len(fields) > 1:
            return tuple(reference[field] for field in fields)
        else:
            return reference[field]

    def extract_basename(self, headers):
        basenames = set()
        for header in headers:
            match = EXTR.match(header['FILENAME'])
            if match is None:
                raise RuntimeError(
                    '{0} does not look like a valid TESS basename'.format(
                        header['FILENAME']
                    )
                )
            basenames.add(match.groupdict()['basename'])
        assert len(basenames) == 1
        return basenames.pop()

    def parse(self, headers):
        echo('Checking congruence of {0} objects'.format(len(headers)))
        if 'orbit_id' in self.context:
            assert all(
                self.context['orbit_id'] == header['ORBIT_ID']
                for header in headers
            )
            orbit_number = self.context['orbit_id']
        else:
            orbit_number = self.check_congruence(headers, 'ORBIT_ID')

        echo('Validating orbit {0} for sector {1}'.format(
            orbit_number, self.context['sector']
        ))

        ra, dec, roll = self.check_congruence(
            headers,
            'SC_RA',
            'SC_DEC',
            'SC_ROLL'
        )
        qx, qy, qz, qq = self.check_congruence(
            headers, 'SC_QUATX', 'SC_QUATY', 'SC_QUATZ', 'SC_QUATQ'
        )
        crm, crm_n = self.check_congruence(headers, 'CRM', 'CRM_N')

        basename = self.extract_basename(headers)
        yield {
            'orbit_number': orbit_number,
            'sector': self.context['sector'],
            'right_ascension': ra,
            'declination': dec,
            'roll': roll,
            'quaternion_x': qx,
            'quaternion_y': qy,
            'quaternion_z': qz,
            'quaternion_q': qq,
            'crm': crm,
            'crm_n': crm_n,
            'basename': basename
        }
