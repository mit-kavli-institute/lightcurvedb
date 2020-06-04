from __future__ import absolute_import, division, print_function

import numpy as np
from lightcurvedb import db_from_config
from lightcurvedb.models.frame import Frame, FrameType, FRAME_DTYPE
from lightcurvedb.models.orbit import Orbit, ORBIT_DTYPE
from sqlalchemy.sql.expression import func


LEGACY_FRAME_TYPE_ID = 'Raw FFI'

FRAME_COMP_DTYPE = [('orbit_id', np.int32)] + FRAME_DTYPE


class QlpQuery(object):
    """
        This class is for legacy implementation of an older lightcurve
        database. The methods here are mimicking older behavior for
        compatibility.
    """

    def __init__(self, conn=None, dbinfo_file=None, dbinfo=None):
        # Ignore most properties, instantiate a connection
        if conn is not None:
            self.db = conn
        else:
            self.db = db_from_config(dbinfo_file)

    def __enter__(self):
        self.db.open()
        return self

    def __exit__(self, *args):
        self.db.close()
        return self

    def query_orbits_by_id(self, orbit_ids):
        """
                Grab a numpy array representing the orbits.
        """
        orbits = self.db.query(*Orbit.get_legacy_attrs()).\
            filter(Orbit.orbit_number.in_(orbit_ids)).\
            order_by(Orbit.orbit_number)
        return np.array(orbits.all(), dtype=ORBIT_DTYPE)

    def query_orbit_cadence_limit(self, orbit_id, cadence_type, camera):
        cadence_limit = self.db.query(
            func.min(Frame.cadence), func.max(Frame.cadence)
        ).join(Frame.orbit).filter(
            Frame.cadence_type == cadence_type,
            Frame.camera == camera,
            Orbit.orbit_number == orbit_id
        )

        return cadence_limit.one()

    def query_orbit_tjd_limit(self, orbit_id, cadence_type, camera):
        tjd_limit = self.db.query(
            func.min(Frame.start_tjd), func.max(Frame.end_tjd)
        ).join(Frame.orbit).filter(
            Frame.cadence_type == cadence_type,
            Frame.camera == camera,
            Orbit.orbit_number == orbit_id
        )

        return tjd_limit.one()

    def query_frames_by_orbit(self, orbit_id, cadence_type, camera):
        # Differs from PATools in that orbit_id != orbit number
        # so we need to record that.
        values = self.db.query(
            [Orbit.orbit_number, *Frame.get_legacy_attrs()]
        ).join(Frame.orbit).filter(
            Frame.cadence_type == cadence_type,
            Frame.camera == camera,
            Orbit.orbit_number == orbit_id
        )

        return np.array(
            values, dtype=FRAME_COMP_DTYPE
        )

    def query_frames_by_cadence(self, camera, cadence_type, cadences):
        values = self.db.query(
            [Orbit.orbit_number, *Frame.get_legacy_attrs()]
        ).join(Frame.orbit).filter(
            Frame.cadence_type == cadence_type,
            Frame.camera == camera,
            Frame.cadence.in_(cadences)
        )

        return np.array(
            values, dtype=FRAME_COMP_DTYPE
        )

    def query_all_orbit_ids(self):
        return self.db.query(Orbit.orbit_number).all()
