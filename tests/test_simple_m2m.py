"""Simple test for many-to-many relationship."""

import numpy as np
from sqlalchemy import orm

from lightcurvedb.models import FITSFrame, Instrument, Observation


def test_basic_m2m(v2_db: orm.Session):
    """Test basic many-to-many functionality."""
    # Create observation
    obs = Observation(
        type="test",
        cadence_reference=np.array([1], dtype=np.int64),
    )
    v2_db.add(obs)
    v2_db.flush()

    # Create frame
    frame = FITSFrame(
        type="test",
        cadence=1,
        simple=True,
        bitpix=16,
        naxis=2,
        naxis_values=[1, 1],
        extend=False,
    )
    v2_db.add(frame)
    v2_db.flush()

    # Associate
    obs.fits_frames.append(frame)
    v2_db.flush()

    # Check
    assert len(obs.fits_frames) == 1
    assert len(frame.observations) == 1
